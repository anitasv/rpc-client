package com.inmobi.adserve;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Makes a composite RpcService out of a list of RpcService(s). When a call is made, it
 * is forwarded to the least loaded among the RpcServices. If all backends are equally
 * loaded all the time, then the behaviour reduces to Round Robin. If bad backends return
 * much faster than good servers, this may not be the right kind of balancing that is
 * needed, because it will increase the chance of new requests going to bad server. If
 * you could detect the bad servers and mark the backend as unhealthy promptly this is
 * not an issue.
 *
 * @param <Req> Request object type.
 * @param <Resp> Response object type.
 */
public class LeastLoaded<Req, Resp> implements Function<Req, ListenableFuture<Resp>> {

    private final Executor executor;

    private final AtomicInteger loopCounter;

    private class RpcWrapper {

        final RpcService<Req, Resp> service;
        final AtomicInteger outboundRequests = new AtomicInteger(0);

        public RpcWrapper(RpcService<Req, Resp> service) {
            this.service = service;
        }

        public ListenableFuture<Resp> call(final Req req) {
            outboundRequests.incrementAndGet();
            final ListenableFuture<Resp> serverFuture = service.getService().apply(req);

            final SettableFuture<Resp> clientFuture = SettableFuture.create();

            serverFuture.addListener(IdempotentRunnable.from(() -> {
                try {
                    Preconditions.checkState(serverFuture.isDone());

                    if (serverFuture.isCancelled()) {
                        clientFuture.cancel(false);
                    } else {
                        try {
                            clientFuture.set(serverFuture.get());
                        } catch (Exception e) {
                            clientFuture.setException(e);
                        }
                    }
                } finally {
                    outboundRequests.decrementAndGet();
                }
            }), executor);

            clientFuture.addListener(IdempotentRunnable.from(() -> {
                if (clientFuture.isCancelled()) {
                    serverFuture.cancel(true);
                }
            }), executor);

            return clientFuture;
        }

        public boolean isHealthy() {
            return service.getHealthInspector().get();
        }
    }

    private final List<RpcWrapper> backends;

    public LeastLoaded(List<RpcService<Req, Resp>> backends,
                       Executor executor) {
        Preconditions.checkArgument(!backends.isEmpty(), "At least one backend must be present");
        this.backends = ImmutableList.<RpcWrapper>builder()
                .addAll(Iterables.transform(backends, RpcWrapper::new)).build();
        this.executor = executor;
        this.loopCounter = new AtomicInteger(ThreadLocalRandom.current().nextInt(backends.size()));
    }

    public ListenableFuture<Resp> apply(Req req) {
        RpcWrapper host = select();
        if (host != null) {
            return host.call(req);
        } else {
            return Futures.immediateFailedFuture(new RpcException("No healthy servers"));
        }
    }

    public boolean isHealthy() {
        for (RpcWrapper rpcWrapper : backends) {
            if (rpcWrapper.isHealthy()) {
                return true;
            }
        }
        return false;
    }

    private RpcWrapper select() {

        int cost = Integer.MAX_VALUE;
        RpcWrapper ret = null;
        int size = backends.size();

        int start = loopCounter.get();
        int nextLoopCounter = start;

        for (int i = 0; i < size; i++) {
            int j = (start + i) % size;
            RpcWrapper rpcWrapper = backends.get(j);
            if (rpcWrapper.isHealthy()) {
                int rpcCost = rpcWrapper.outboundRequests.get();
                if (rpcCost < cost) {
                    cost = rpcCost;
                    ret = rpcWrapper;
                    nextLoopCounter = j + 1;
                }
            }
        }

        loopCounter.set(nextLoopCounter);

        return ret;
    }

    public RpcService<Req, Resp> asService() {
        return new RpcService<Req, Resp>(this, this::isHealthy);
    }
}
