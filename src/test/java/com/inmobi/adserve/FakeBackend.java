package com.inmobi.adserve;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FakeBackend implements RpcService<Object, Object> {

    private final AsyncQueue asyncQueue = new AsyncQueue();

    private final ScheduledExecutorService executorService;

    private final long cpuTimeMillis;
    private AtomicInteger numRequests = new AtomicInteger(0);

    public FakeBackend(ScheduledExecutorService executorService, long cpuTimeMillis) {
        this.executorService = executorService;
        this.cpuTimeMillis = cpuTimeMillis;
    }

    @Override
    public boolean isHealthy() {
        return true;
    }

    @Override
    public ListenableFuture<Object> apply(Object o) {
        SettableFuture<Object> ret = SettableFuture.create();
        asyncQueue.submit(completionHook -> {
                executorService.schedule(() -> {
                            ret.set(new Object());
                            numRequests.incrementAndGet();
                            completionHook.run();
                        },
                        cpuTimeMillis, TimeUnit.MILLISECONDS);
                }
        );
        return ret;
    }

    public int getNumRequests() {
        return numRequests.get();
    }
}
