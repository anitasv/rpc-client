package com.inmobi.rpc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.inmobi.rpc.RpcTestUtils.*;
import static org.testng.Assert.*;

public class LeastLoadedTest {

    @Test
    public void testOneBackendSuccessBlocking() throws ExecutionException, InterruptedException {
        Object req = new Object();
        Object resp = new Object();

        RpcService<Object, Object> healthy = immediateSuccess(resp);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        assertTrue(leastLoaded.isHealthy());
        assertEquals(leastLoaded.apply(req).get(), resp, "Response must match what is sent by backend");
        assertTrue(leastLoaded.isHealthy());

    }

    @Test
    public void testOneBackendSuccessAsync() throws ExecutionException, InterruptedException {
        Object req = new Object();
        Object resp = new Object();

        RpcService<Object, Object> healthy = immediateSuccess(resp);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        assertTrue(leastLoaded.isHealthy());
        ListenableFuture<Object> future = leastLoaded.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        wait.acquire();
        assertEquals(future.get(), resp, "Response must match what is sent by backend");
        assertTrue(leastLoaded.isHealthy());
    }

    @Test(expectedExceptions = CancellationException.class)
    public void testOneBackendFailBlocking() throws ExecutionException, InterruptedException {
        Object req = new Object();

        RpcService<Object, Object> healthy = immediateFail();

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        assertTrue(leastLoaded.isHealthy());
        leastLoaded.apply(req).get();
        fail();
    }

    @Test
    public void testOneBackendFailAsync() throws ExecutionException, InterruptedException {
        Object req = new Object();

        RpcService<Object, Object> healthy = immediateFail();

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        assertTrue(leastLoaded.isHealthy());
        ListenableFuture<Object> future = leastLoaded.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        wait.acquire();
        assertTrue(leastLoaded.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendServerDelayedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        assertTrue(leastLoaded.isHealthy());
        ListenableFuture<Object> future = leastLoaded.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        serverFuture.cancel(true);
        wait.acquire();
        assertTrue(leastLoaded.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendServerTimedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        assertTrue(leastLoaded.isHealthy());
        ListenableFuture<Object> future = leastLoaded.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(() -> {
            serverFuture.cancel(true);
        }, 1, TimeUnit.SECONDS);

        wait.acquire();
        assertTrue(leastLoaded.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testOneBackendClientCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        ListenableFuture<Object> future = leastLoaded.apply(req);
        future.cancel(true);

        serverFuture.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();

        assertTrue(leastLoaded.isHealthy());
        assertTrue(serverFuture.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendClientDelayedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(healthy),
                MoreExecutors.directExecutor());

        Semaphore wait = new Semaphore(0);

        ListenableFuture<Object> future = leastLoaded.apply(req);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(() -> {
            future.cancel(true);
        }, 1, TimeUnit.SECONDS);


        serverFuture.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();

        assertTrue(leastLoaded.isHealthy());
        assertTrue(serverFuture.isCancelled(), "Client future must be in cancelled state");
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testRoundRobinBehaviourWhenNotLoaded() throws ExecutionException, InterruptedException {
        Object req = new Object();

        AtomicInteger resp1 = new AtomicInteger(0);
        AtomicInteger resp2 = new AtomicInteger(0);
        AtomicInteger resp3 = new AtomicInteger(0);

        RpcService<Object, AtomicInteger> backend1 = immediateSuccess(resp1);
        RpcService<Object, AtomicInteger> backend2 = immediateSuccess(resp2);
        RpcService<Object, AtomicInteger> backend3 = immediateSuccess(resp3);

        LeastLoaded<Object, AtomicInteger> leastLoaded = new LeastLoaded<>(
                ImmutableList.of(backend1, backend2, backend3),
                MoreExecutors.directExecutor());

        assertTrue(leastLoaded.isHealthy());
        assertEquals(leastLoaded.apply(req).get().incrementAndGet(), 1);
        assertEquals(leastLoaded.apply(req).get().incrementAndGet(), 1);
        assertEquals(leastLoaded.apply(req).get().incrementAndGet(), 1);
        assertEquals(resp1.get() + resp2.get() + resp3.get(), 3);
    }

    @Test
    public void testLeastLoadedBehaviourForPoissonProcess() throws ExecutionException, InterruptedException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        long delay1 = 10;
        long delay2 = 20;
        long delay3 = 30;

        FakeBackend backend1 = new FakeBackend(scheduler, delay1);
        FakeBackend backend2 = new FakeBackend(scheduler, delay2);
        FakeBackend backend3 = new FakeBackend(scheduler, delay3);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<Object, Object>(
                ImmutableList.<RpcService<Object, Object>>of(backend1, backend2, backend3),
                MoreExecutors.directExecutor());

        assertTrue(leastLoaded.isHealthy());

        double duration = TimeUnit.SECONDS.toMillis(1);
        double rate = (duration / delay1) + (duration / delay2) + (duration / delay3);
        int numRequests = (int) Math.floor(rate);

        long[] arrivalRates = new long[numRequests];
        ThreadLocalRandom current = ThreadLocalRandom.current();

        for (int i = 0; i < numRequests; i++) {
            arrivalRates[i] = current.nextLong(TimeUnit.SECONDS.toNanos(1));
        }

        CountDownLatch countDownLatch = new CountDownLatch(numRequests);

        Arrays.sort(arrivalRates);

        Stopwatch watch = Stopwatch.createStarted();
        for (int i = 0; i < numRequests; i++) {
            scheduler.schedule(() -> {
                leastLoaded.apply(new Object())
                        .addListener(countDownLatch::countDown, MoreExecutors.directExecutor());
            }, arrivalRates[i], TimeUnit.NANOSECONDS);
        }
        countDownLatch.await();

        double achievedRate = numRequests * 1e9 / watch.elapsed(TimeUnit.NANOSECONDS);
        System.err.println("LeastLoadedRR: Throughput (Expected = " + rate + " ) (Actual = " + achievedRate + ")" );

        assertTrue(backend1.getNumRequests() > backend2.getNumRequests(), "Backend 1 must process more requests than backend 2");
        assertTrue(backend2.getNumRequests() > backend3.getNumRequests(), "Backend 2 must process more requests than backend 3");
        scheduler.shutdown();
   }

    @Test(expectedExceptions = ExecutionException.class)
    public void testFailedBehaviour() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(backend1),
                MoreExecutors.directExecutor());

        leastLoaded.apply(new Object()).get();
        fail();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testFailedBehaviourAsync() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(backend1),
                MoreExecutors.directExecutor());

        ListenableFuture<Object> future = leastLoaded.apply(new Object());
        Semaphore wait = new Semaphore(0);
        future.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();
        assertTrue(future.isDone());
        future.get();
        fail();
    }

    @Test
    public void testFailedLeastLoadedBehaviour() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));
        Object resp = new Object();
        RpcService<Object, Object> backend2 = immediateSuccess(resp);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(ImmutableList.of(backend1, backend2),
                MoreExecutors.directExecutor());

        while (true) {
            try {
                assertEquals(leastLoaded.apply(new Object()).get(), resp, "We should eventually get right response");
                return;
            } catch (ExecutionException ignored) {
            }
        }
    }
}
