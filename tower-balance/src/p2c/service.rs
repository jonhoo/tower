use crate::error;
use futures::{future, stream, try_ready, Async, Future, Poll, Stream};
use indexmap::IndexMap;
use tracing::{debug, info, trace};
use rand::{rngs::SmallRng, FromEntropy};
use tokio_sync::oneshot;
use tower::ServiceExt;
use tower_discover::{Change, Discover};
use tower_load::Load;
use tower_service::Service;

/// Distributes requests across inner services using the [Power of Two Choices][p2c].
///
/// As described in the [Finagle Guide][finagle]:
///
/// > The algorithm randomly picks two services from the set of ready endpoints and
/// > selects the least loaded of the two. By repeatedly using this strategy, we can
/// > expect a manageable upper bound on the maximum load of any server.
/// >
/// > The maximum load variance between any two servers is bound by `ln(ln(n))` where
/// > `n` is the number of servers in the cluster.
///
/// [finagle]: https://twitter.github.io/finagle/guide/Clients.html#power-of-two-choices-p2c-least-loaded
/// [p2c]: http://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf
#[derive(Debug)]
pub struct Balance<D: Discover, Req> {
    discover: D,

    ready: IndexMap<D::Key, D::Service>,

    unready: stream::FuturesUnordered<CancelReady<D::Key, D::Service, Req>>,
    cancelations: IndexMap<D::Key, oneshot::Sender<()>>,

    /// Holds an index into `endpoints`, indicating the service that has been
    /// chosen to dispatch the next request.
    next_ready_index: Option<usize>,

    rng: SmallRng,
}

#[derive(Debug)]
struct CancelReady<K, S, Req> {
    key: Option<K>,
    cancel: oneshot::Receiver<()>,
    ready: tower_util::Ready<S, Req>,
}

enum Error<E> {
    Inner(E),
    Canceled,
}

impl<D, Req> Balance<D, Req>
where
    D: Discover,
    D::Service: Service<Req>,
{
    /// Initializes a P2C load balancer from the provided randomization source.
    pub fn new(discover: D, rng: SmallRng) -> Self {
        Self {
            rng,
            discover,
            ready: IndexMap::default(),
            cancelations: IndexMap::default(),
            unready: stream::FuturesUnordered::new(),
            next_ready_index: None,
        }
    }

    /// Initializes a P2C load balancer from the OS's entropy source.
    pub fn from_entropy(discover: D) -> Self {
        Self::new(discover, SmallRng::from_entropy())
    }

    /// Returns the number of endpoints currently tracked by the balancer.
    pub fn len(&self) -> usize {
        self.ready.len() + self.unready.len()
    }

    // XXX `pool::Pool` requires direct access to this... Not ideal.
    pub(crate) fn discover_mut(&mut self) -> &mut D {
        &mut self.discover
    }
}

impl<D, Req> Balance<D, Req>
where
    D: Discover,
    D::Key: Clone,
    D::Error: Into<error::Error>,
    D::Service: Service<Req> + Load,
    <D::Service as Load>::Metric: std::fmt::Debug,
    <D::Service as Service<Req>>::Error: Into<error::Error>,
{
    /// Polls `discover` for updates, adding new items to `not_ready`.
    ///
    /// Removals may alter the order of either `ready` or `not_ready`.
    fn poll_discover(&mut self) -> Poll<(), error::Discover> {
        debug!("updating from discover");
        loop {
            match try_ready!(self.discover.poll().map_err(|e| error::Discover(e.into()))) {
                Change::Remove(key) => {
                    trace!("remove");
                    self.evict(&key)
                }
                Change::Insert(key, svc) => {
                    trace!("insert");
                    self.evict(&key);
                    self.push_unready(key, svc);
                }
            }
        }
    }

    fn push_unready(&mut self, key: D::Key, svc: D::Service) {
        let (tx, rx) = oneshot::channel();
        self.cancelations.insert(key.clone(), tx);
        self.unready.push(CancelReady {
            key: Some(key),
            ready: svc.ready(),
            cancel: rx,
        });
    }

    fn evict(&mut self, key: &D::Key) {
        // Update the ready index to account for reordering of ready.
        if let Some((idx, _, _)) = self.ready.swap_remove_full(key) {
            self.next_ready_index = self
                .next_ready_index
                .and_then(|i| Self::repair_index(i, idx, self.ready.len()));
            debug_assert!(!self.cancelations.contains_key(key));
        } else if let Some(cancel) = self.cancelations.remove(key) {
            let _ = cancel.send(());
        }
    }

    fn poll_unready(&mut self) {
        loop {
            match self.unready.poll() {
                Ok(Async::NotReady) | Ok(Async::Ready(None)) => return,
                Ok(Async::Ready(Some((key, svc)))) => {
                    trace!("endpoint ready");
                    let _cancel = self.cancelations.remove(&key);
                    debug_assert!(_cancel.is_some(), "missing cancelation");
                    self.ready.insert(key, svc);
                }
                Err((key, Error::Canceled)) => debug_assert!(!self.cancelations.contains_key(&key)),
                Err((key, Error::Inner(e))) => {
                    let error = e.into();
                    info!({ ?error }, "dropping failed endpoint");
                    let _cancel = self.cancelations.swap_remove(&key);
                    debug_assert!(_cancel.is_some());
                }
            }
        }
    }

    // Returns the updated index of `orig_idx` after the entry at `rm_idx` was
    // swap-removed from an IndexMap with `orig_sz` items.
    //
    // If `orig_idx` is the same as `rm_idx`, None is returned to indicate that
    // index cannot be repaired.
    fn repair_index(orig_idx: usize, rm_idx: usize, new_sz: usize) -> Option<usize> {
        debug_assert!(orig_idx <= new_sz && rm_idx <= new_sz);
        let repaired = match orig_idx {
            i if i == rm_idx => None,         // removed
            i if i == new_sz => Some(rm_idx), // swapped
            i => Some(i),                     // uneffected
        };
        trace!(
            { orig = orig_idx, rm = rm_idx, sz = new_sz, i = ?repaired },
            "repair_index"
        );
        repaired
    }

    /// Performs P2C on inner services to find a suitable endpoint.
    fn poll_next_ready_index(&mut self) -> Poll<(), error::Error> {
        self.next_ready_index = match self.ready.len() {
            0 => None,
            1 => self.poll_ready_index_load(0).map(|_| 0),
            len => {
                // Get two distinct random indexes (in a random order). Poll the
                // service at each index.
                //
                // If either fails, the service is removed.
                let idxs = rand::seq::index::sample(&mut self.rng, len, 2);

                let aidx = idxs.index(0);
                let bidx = idxs.index(1);
                debug_assert_ne!(aidx, bidx, "random indices must be distinct");

                let (aload, bidx) = match self.poll_ready_index_load(aidx) {
                    Some(aload) => (Some(aload), bidx),
                    None => {
                        let new_bidx = Self::repair_index(bidx, aidx, self.ready.len())
                            .expect("random indices must be distinct");
                        (None, new_bidx)
                    }
                };

                let (bload, aidx) = match self.poll_ready_index_load(bidx) {
                    Some(bload) => (Some(bload), aidx),
                    None => {
                        let new_aidx = Self::repair_index(aidx, bidx, self.ready.len())
                            .expect("random indices must be distinct");
                        (None, new_aidx)
                    }
                };

                trace!({ ai = aidx, al = ?aload, bi = bidx, bl = ?bload }, "choosing by load");

                let ready = match (aload, bload) {
                    (Some(aload), Some(bload)) => {
                        if aload <= bload {
                            Some(aidx)
                        } else {
                            Some(bidx)
                        }
                    }
                    (a, b) => a.map(|_| aidx).or_else(|| b.map(|_| bidx)),
                };
                trace!({ ?ready }, "ready");
                ready
            }
        };

        if self.next_ready_index.is_some() {
            return Ok(Async::Ready(()));
        }

        Ok(Async::NotReady)
    }

    /// Accesses a ready endpoint by index and returns its current load.
    fn poll_ready_index_load(&mut self, index: usize) -> Option<<D::Service as Load>::Metric> {
        let (_, svc) = self.ready.get_index_mut(index).expect("invalid index");
        let load = match svc.poll_ready() {
            Ok(Async::Ready(_)) => Some(svc.load()),
            Ok(Async::NotReady) => {
                // became unready; so move it back there.
                let (key, svc) = self
                    .ready
                    .swap_remove_index(index)
                    .expect("invalid ready index");
                self.push_unready(key, svc);
                None
            }
            Err(e) => {
                // failed, so drop it.
                let error = e.into();
                info!({ ?error }, "evicting failed endpoint");
                self.ready
                    .swap_remove_index(index)
                    .expect("invalid ready index");
                None
            }
        };
        trace!({ index, ?load }, "poll_ready_index_load");
        load
    }
}

impl<D, Req> Service<Req> for Balance<D, Req>
where
    D: Discover,
    D::Key: Clone,
    D::Error: Into<error::Error>,
    D::Service: Service<Req> + Load,
    <D::Service as Load>::Metric: std::fmt::Debug,
    <D::Service as Service<Req>>::Error: Into<error::Error>,
{
    type Response = <D::Service as Service<Req>>::Response;
    type Error = error::Error;
    type Future = future::MapErr<
        <D::Service as Service<Req>>::Future,
        fn(<D::Service as Service<Req>>::Error) -> error::Error,
    >;

    /// Prepares the balancer to process a request.
    ///
    /// When `Async::Ready` is returned, `ready_index` is set with a valid index
    /// into `ready` referring to a `Service` that is ready to disptach a request.
    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        // First and foremost, process discovery updates. This removes or updates a
        // previously-selected `ready_index` if appropriate.
        self.poll_discover()?;

        self.poll_unready();
        debug!({ nready = self.ready.len(), nunready = self.unready.len() }, "poll_ready");

        if let Some(index) = self.next_ready_index {
            trace!({ index }, "preselected ready_index");
            debug_assert!(index < self.ready.len());
            // Ensure the selected endpoint is still ready.
            if self.poll_ready_index_load(index).is_some() {
                return Ok(Async::Ready(()));
            }

            self.next_ready_index = None;
        }

        self.poll_next_ready_index()
    }

    fn call(&mut self, request: Req) -> Self::Future {
        let index = self.next_ready_index.take().expect("not ready");
        let (key, mut svc) = self
            .ready
            .swap_remove_index(index)
            .expect("invalid ready index");
        // no need to repair since the ready_index has been cleared.

        let fut = svc.call(request);
        self.push_unready(key, svc);

        fut.map_err(Into::into)
    }
}

impl<K, S: Service<Req>, Req> Future for CancelReady<K, S, Req> {
    type Item = (K, S);
    type Error = (K, Error<S::Error>);

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Ok(Async::Ready(())) = self.cancel.poll() {
            let key = self.key.take().expect("polled after ready");
            return Err((key, Error::Canceled));
        }

        match self.ready.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(svc)) => {
                let key = self.key.take().expect("polled after ready");
                Ok((key, svc).into())
            }
            Err(e) => {
                let key = self.key.take().expect("polled after ready");
                Err((key, Error::Inner(e)))
            }
        }
    }
}