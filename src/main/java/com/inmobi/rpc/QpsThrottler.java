package com.inmobi.rpc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QpsThrottler<Req, Resp> implements RpcService<Req, Resp> {

    private final AtomicInteger counter;

    private final RpcService<Req, Resp> backend;

    private final Clock clock;

    private final int maxQueries;

    private final long durationNanos;

    private final AtomicInteger prevSlot = new AtomicInteger();

    public QpsThrottler(RpcService<Req, Resp> backend,
                        int maxQueries,
                        int duration,
                        TimeUnit unit,
                        Clock clock) {
        this.backend = backend;
        this.counter = new AtomicInteger(0);
        this.clock = clock;
        this.durationNanos = unit.toNanos(duration);
        this.maxQueries = maxQueries;
    }

    @Override
    public boolean isHealthy() {
        return hasPermit() && backend.isHealthy();
    }

    @Override
    public ListenableFuture<Resp> apply(Req req) {
        if (tryAcquire()) {
            try {
                return backend.apply(req);
            } catch (Exception e) {
                return Futures.immediateFailedFuture(e);
            }
        } else {
            return Futures.immediateCancelledFuture();
        }
    }

    private AtomicInteger slot() {
        long instant = clock.instant().getNano();
        int bucket = (int) ((instant / durationNanos));

        int prevSlotId = prevSlot.get();
        if (prevSlotId < bucket) {
            counter.set(0);
        }
        return this.counter;
    }

    private boolean hasPermit() {
        return slot().get() < maxQueries;
    }

    private boolean tryAcquire() {
        AtomicInteger slot = slot();
        do {
            int queries = slot.get();
            if (queries < maxQueries) {
                if (slot.compareAndSet(queries, queries + 1)) {
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

}
