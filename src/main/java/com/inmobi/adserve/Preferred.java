package com.inmobi.adserve;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class Preferred<Req, Resp> implements RpcService<Req, Resp> {

    private final List<RpcService<Req, Resp>> backends;

    public Preferred(List<RpcService<Req, Resp>> backends) {
        this.backends = backends;
    }

    @Override
    public boolean isHealthy() {
        for (RpcService<Req, Resp> rpcService : backends) {
            if (rpcService.isHealthy()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ListenableFuture<Resp> apply(Req req) {
        for (RpcService<Req, Resp> rpcService : backends) {
            if (rpcService.isHealthy()) {
                return rpcService.apply(req);
            }
        }
        return Futures.immediateFailedFuture(new RpcException("No healthy servers"));
    }
}
