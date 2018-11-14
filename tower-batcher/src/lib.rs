//! This crate provides middleware that submits batches of requests to a backend service.
//!
//! Or, stated differently, it wraps a service (well, `DirectService`) of type `fn(RequestBatch) ->
//! impl Future<ResponseBatch>`, and provides an interface of type `fn(Stream<Request>) ->
//! impl Stream(Response)`.
//!
//! When you `call` [`BatchingService`], you provide a stream of requests, rather than individual
//! requests. [`BatchingService`] accumulates requests across all its request streams into a batch,
//! and sends the entire batch to its backing service whenever any request batch completes (i.e.,
//! when any request stream yields `None`). The motivation for this is that clients may, at some
//! point, wish to wait for responses to arrive before continuing, but no responses will arrive
//! until the batch has been sent. Therefore, the caller has to have a way of indicating that they
//! wish to wait to send the batch and wait for responses to arrive.

#![deny(missing_docs)]

#[macro_use]
extern crate futures;
extern crate streamunordered;
extern crate tower_direct_service;
extern crate tower_service;

use futures::sync::mpsc;
use futures::{Async, Future, Poll, Stream};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use streamunordered::*;
use tower_direct_service::DirectService;

struct WithOffset<F> {
    offset: usize,
    fut: F,
}

impl<F> Future for WithOffset<F>
where
    F: Future,
{
    type Item = (usize, F::Item);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready((self.offset, try_ready!(self.fut.poll()))))
    }
}

struct StreamState<T> {
    requests: usize,
    responses: usize,
    done: bool,
    tx: mpsc::UnboundedSender<(usize, T)>,
}

/// The batching middleware provided by this crate.
///
/// It takes streams of type `Requests`, accumulates their items into a `Batch`, and then sends
/// any flushed `Batch` to the service `S`. When `S` finishes processing a batch, the responses for
/// each request in the batch are sent on the channel returned by `call`. The responses also
/// include the batch number, so that applications can detect if responses arrive out-of-order.
pub struct BatchingService<Requests, Batch, S>
where
    S: DirectService<Batch>,
    S::Response: IntoIterator,
{
    requests: StreamUnordered<Requests>,
    for_response: VecDeque<usize>,
    waiting: VecDeque<(usize, Option<WithOffset<S::Future>>)>,
    streams: HashMap<usize, StreamState<<S::Response as IntoIterator>::Item>>,
    accum: Batch,
    retired: usize,
    batches: usize,
    flush: bool,
    closing: bool,
    max_batch_size: usize,
    service: S,
}

impl<Requests, Batch, S> From<S> for BatchingService<Requests, Batch, S>
where
    Batch: Default,
    S: DirectService<Batch>,
    S::Response: IntoIterator,
{
    fn from(service: S) -> Self {
        BatchingService {
            requests: Default::default(),
            for_response: Default::default(),
            waiting: Default::default(),
            streams: Default::default(),
            accum: Default::default(),
            retired: 0,
            batches: 0,
            flush: false,
            closing: false,
            max_batch_size: 0,
            service,
        }
    }
}

impl<Requests, Batch, S> BatchingService<Requests, Batch, S>
where
    S: DirectService<Batch>,
    S::Response: IntoIterator,
{
    /// Force a flush after `max` items have been accumulated in a batch.
    ///
    /// The default is `None`, which implies no forced flushing. `Some(0)` has the same meaning.
    pub fn force_flush_after(mut self, max: Option<usize>) -> Self {
        self.max_batch_size = max.unwrap_or(0);
        self
    }
}

/// Implementors of this trait can be used as the `Batch` for a [`BatchingService`]. Unless you
/// have a particularly good reason not to, you should probably just use a `Vec`.
pub trait Extensible {
    /// The type of element this batch can be extended with.
    type Item;

    /// The number of items in this batch.
    fn len(&self) -> usize;

    /// Returns true if this batch contains no items (equivalent to `.len() == 0`).
    fn is_empty(&self) -> bool;

    /// Add the given element `e` to this batch.
    fn push(&mut self, e: Self::Item);
}

impl<T> Extensible for Vec<T> {
    type Item = T;

    fn len(&self) -> usize {
        self.len()
    }
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
    fn push(&mut self, e: Self::Item) {
        self.push(e)
    }
}

impl<Requests, Batch, S> DirectService<Requests> for BatchingService<Requests, Batch, S>
where
    Requests: Stream,
    Batch: Extensible<Item = Requests::Item> + Default,
    S: DirectService<Batch>,
    S::Response: IntoIterator,
    S::Error: From<Requests::Error>,
{
    type Response = mpsc::UnboundedReceiver<(usize, <S::Response as IntoIterator>::Item)>;
    type Error = S::Error;
    type Future = futures::future::FutureResult<Self::Response, Self::Error>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        self.service.poll_ready()
    }

    fn poll_close(&mut self) -> Poll<(), Self::Error> {
        self.closing = true;
        self.poll_service()
    }

    fn poll_service(&mut self) -> Poll<(), Self::Error> {
        // check for more requests
        loop {
            match self.requests.poll()? {
                Async::Ready(Some((item, si))) => {
                    match item {
                        StreamYield::Item(req) => {
                            // track the fact that the response at this index maps to this stream
                            self.for_response.push_back(si);

                            // and that this stream has one more requests (to know when we're done)
                            self.streams
                                .get_mut(&si)
                                .expect("got request from unknown stream")
                                .requests += 1;

                            // add the request to the next batch
                            self.accum.push(req);

                            // is the batch getting large enough to warrant an eager flush?
                            if self.max_batch_size != 0 && self.accum.len() >= self.max_batch_size {
                                self.flush = true;
                            }
                        }
                        StreamYield::Finished(_) => {
                            // if we have any elements in the batch, we need to flush now, since
                            // the caller has finished the batch, and is probably waiting for
                            // responses.
                            self.flush = !self.accum.is_empty();

                            self.streams
                                .get_mut(&si)
                                .expect("unknown stream finished")
                                .done = true;
                        }
                    }
                }
                Async::NotReady => {
                    // no more requests for now, so we might as well flush
                    self.flush = !self.accum.is_empty();
                    break;
                }
                Async::Ready(None) => {
                    // there are no more requests for now
                    break;
                }
            }
        }

        // flush if we have a batch that needs to go out
        if !self.accum.is_empty() && self.flush {
            if let Async::Ready(()) = self.service.poll_ready()? {
                let n = self.accum.len();
                let fut = self
                    .service
                    .call(mem::replace(&mut self.accum, Default::default()));

                // keep track of the batch index so we can detect when responses come back out of
                // order
                let fut = WithOffset {
                    offset: self.batches,
                    fut,
                };
                self.waiting.push_back((n, Some(fut)));
                self.flush = false;
                self.batches += 1;
            }
        }

        // see if there are any responses for us
        let mut offset = 0;
        for (n, fut) in self.waiting.iter_mut() {
            if let Some(mut f) = fut.take() {
                match f.poll()? {
                    Async::Ready((batch, rsp)) => {
                        for (i, rsp) in rsp.into_iter().enumerate() {
                            let stream_i = self.for_response[offset + i - self.retired];

                            // NLL plz
                            let rm = {
                                let rsps = self
                                    .streams
                                    .get_mut(&stream_i)
                                    .expect("got request for removed stream");

                                // TODO: optimize send many?
                                rsps.responses += 1;
                                if let Err(_) = rsps.tx.unbounded_send((batch, rsp)) {
                                    // receiver doesn't care about this batch any more
                                    // that's fine...
                                }

                                rsps.done && rsps.responses == rsps.requests
                            };

                            if rm {
                                self.streams.remove(&stream_i);
                            }
                        }
                    }
                    Async::NotReady => {
                        *fut = Some(f);
                    }
                }
            }

            offset += *n;
        }

        // get rid of responses we've now finished with
        while let Some(w) = self.waiting.pop_front() {
            if w.1.is_some() {
                self.waiting.push_front(w);
                break;
            } else {
                // the first w.0 entries of for_response are now unnecessary
                self.retired += w.0;
                self.for_response.drain(0..w.0);
            }
        }

        // let the underlying service decide how to make progress
        if self.closing {
            self.service.poll_close()
        } else {
            self.service.poll_service()
        }
    }

    fn call(&mut self, req: Requests) -> Self::Future {
        let (tx, rx) = mpsc::unbounded();
        let stream_i = self.requests.push(req);
        self.streams.insert(
            stream_i,
            StreamState {
                requests: 0,
                responses: 0,
                done: false,
                tx,
            },
        );

        // TODO: from the streamunordered docs:
        // > `StreamUnordered` will only poll managed streams when `StreamUnordered::poll` is called.
        // > As such, it is important to call poll after pushing new streams.
        // We don't currently do that, and that's probably bad

        futures::future::ok(rx)
    }
}
