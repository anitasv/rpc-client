package com.inmobi.rpc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Semaphore;

public class SemaphoreThrottler<Req, Resp> implements RpcService<Req, Resp> {

    private final Semaphore semaphore;

    private final RpcService<Req, Resp> backend;

    public SemaphoreThrottler(RpcService<Req, Resp> backend,
                              int maxConcurrentRequests) {
        this.semaphore = new Semaphore(maxConcurrentRequests);
        this.backend = backend;
    }

    @Override
    public boolean isHealthy() {
        return semaphore.availablePermits() > 0 && backend.isHealthy();
    }

    @Override
    public ListenableFuture<Resp> apply(Req req) {
        if (semaphore.tryAcquire()) {
            try {
                ListenableFuture<Resp> serverFuture = backend.apply(req);
                serverFuture.addListener(semaphore::release, MoreExecutors.directExecutor());
                return serverFuture;
            } catch (Exception e) {
                semaphore.release();
                return Futures.immediateFailedFuture(e);
            }
        } else {
            return Futures.immediateCancelledFuture();
        }
    }
}

