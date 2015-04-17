package com.inmobi.rpc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Round robin load balancer. Technically under conditions when backend responds faster
 * than inter-arrival time of requests there may be a failure in strict round robin
 * semantics.
 */
public class RoundRobin<Req, Resp> implements RpcService<Req, Resp> {

    private final ImmutableList<RpcService<Req, Resp>> backends;

    private final AtomicInteger rotation;

    public RoundRobin(ImmutableList<RpcService<Req, Resp>> backends,
                      int startingPoint) {
        Preconditions.checkArgument(!backends.isEmpty(), "Backends should not be empty.");
        Preconditions.checkElementIndex(startingPoint, backends.size());;
        this.backends = backends;
        this.rotation = new AtomicInteger(startingPoint);
    }

    public RoundRobin(ImmutableList<RpcService<Req, Resp>> backends) {
        Preconditions.checkArgument(!backends.isEmpty(), "Backends should not be empty.");
        this.backends = backends;
        this.rotation = new AtomicInteger(ThreadLocalRandom.current().nextInt(backends.size()));
    }

    @Override
    public boolean isHealthy() {
        for (RpcService<Req, Resp> backend : backends) {
            if (backend.isHealthy()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ListenableFuture<Resp> apply(Req req) {
        RpcService<Req, Resp> host = select();
        if (host != null) {
            return host.apply(req);
        } else {
            return Futures.immediateFailedFuture(new RpcException("No healthy hosts"));
        }
    }

    private RpcService<Req, Resp> select() {
        int size = backends.size();
        int start = this.rotation.get();
        for (int i = 0; i < size; i++) {
            int pos = (start + i) % size;
            RpcService<Req, Resp> host = backends.get(pos);
            if (host.isHealthy()) {
                this.rotation.set((pos + 1) % size);
                return host;
            }
        }
        return null;
    }

}
