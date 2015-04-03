package com.inmobi.adserve;

import java.util.List;
import java.util.concurrent.Executor;

public class RpcServices {

    private RpcServices() {}

    public static <Req, Resp> RpcService<Req, Resp>  leastLoaded(List<RpcService<Req, Resp>> backends,
                                                                 Executor executor) {
        return new LeastLoaded<>(backends, executor).asService();
    }
}
