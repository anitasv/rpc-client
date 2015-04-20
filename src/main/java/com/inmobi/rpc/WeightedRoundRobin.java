package com.inmobi.rpc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @param <Req>
 * @param <Resp>
 */
public class WeightedRoundRobin <Req, Resp> implements RpcService<Req, Resp> {


    private class RpcWrapper implements RpcService<Req,Resp> {

        private final RpcService<Req, Resp> service;

        private final int weight;

        private final AtomicLong requestsAttempted = new AtomicLong(0);

        public RpcWrapper(RpcService<Req, Resp> service, int weight) {
            this.service = service;
            this.weight = weight;
        }

        @Override
        public boolean isHealthy() {
             return service.isHealthy() && weight != 0;
        }

        @Override
        public ListenableFuture<Resp> apply(Req req) {
            return service.apply(req);
        }

        public double getCost() {
            return weight == 0 ? Double.MAX_VALUE : ((double) requestsAttempted.get()) / weight;
        }

        public void updateCost(double minHealthyCost) {
            if (service.isHealthy()) {
                return;
            }
            // For unhealthy servers to catch up on number of requests.
            long healthyLimit = (long) Math.floor(minHealthyCost * weight);
            requestsAttempted.set(healthyLimit);
        }
    }

    private final ImmutableList<RpcWrapper> backends;

    /**
     * If sum(weights) is too large, it will take that many requests before
     * renormalization starts. So make sure the sum is in the order of requests
     * received in some tolerable time.
     *
     * @param backends
     * @param weights
     */
    public WeightedRoundRobin(ImmutableList<RpcService<Req, Resp>> backends,
                              ImmutableList<Integer> weights) {
        Preconditions.checkArgument(backends.isEmpty(), "At least one backend must be there");
        Preconditions.checkArgument(weights.size() == backends.size(), "Weights must match backend count");

        ImmutableList.Builder<RpcWrapper> builder = ImmutableList.builder();
        for (int i = 0; i < backends.size(); i++) {
            RpcService<Req, Resp> service = backends.get(i);
            int weight = weights.get(i);
            RpcWrapper wrapper = new RpcWrapper(service, weight);
            builder.add(wrapper);
        }
        this.backends = builder.build();
    }

    @Override
    public boolean isHealthy() {
        return false;
    }

    @Override
    public ListenableFuture<Resp> apply(Req req) {
        RpcWrapper wrapper = select();
        if (wrapper == null) {
            return Futures.immediateFailedFuture(new RpcException("No healthy servers"));
        }
        return wrapper.apply(req);
    }

    private RpcWrapper select() {

        RpcWrapper retHealthy = null;

        double minHealthyCost = Double.MAX_VALUE;

        for (RpcWrapper server : backends) {
            double cost = server.getCost();
            if (cost < minHealthyCost && server.isHealthy()) {
                minHealthyCost = cost;
                retHealthy = server;
            }
        }
        for (RpcWrapper candidate : backends) {
            candidate.updateCost(minHealthyCost);
        }
        if (retHealthy != null) {
            retHealthy.requestsAttempted.incrementAndGet();
        }
        return retHealthy;
    }
}
