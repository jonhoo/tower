(function() {var implementors = {};
implementors["tower_service"] = [];
implementors["tower_util"] = [{text:"impl&lt;T, R, S, Request&gt; <a class=\"trait\" href=\"tower_service/trait.NewService.html\" title=\"trait tower_service::NewService\">NewService</a>&lt;Request&gt; for <a class=\"struct\" href=\"tower_util/struct.NewServiceFn.html\" title=\"struct tower_util::NewServiceFn\">NewServiceFn</a>&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>() -&gt; R,<br>&nbsp;&nbsp;&nbsp;&nbsp;R: <a class=\"trait\" href=\"https://docs.rs/futures/0.1/futures/future/trait.IntoFuture.html\" title=\"trait futures::future::IntoFuture\">IntoFuture</a>&lt;Item = S&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: <a class=\"trait\" href=\"tower_service/trait.Service.html\" title=\"trait tower_service::Service\">Service</a>&lt;Request&gt;,&nbsp;</span>",synthetic:false,types:["tower_util::service_fn::NewServiceFn"]},];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
