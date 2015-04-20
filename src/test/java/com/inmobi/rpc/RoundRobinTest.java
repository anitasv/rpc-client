package com.inmobi.rpc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.inmobi.rpc.RpcTestUtils.custom;
import static com.inmobi.rpc.RpcTestUtils.immediateFail;
import static com.inmobi.rpc.RpcTestUtils.immediateSuccess;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RoundRobinTest {

    @Test
    public void testOneBackendSuccessBlocking() throws ExecutionException, InterruptedException {
        Object req = new Object();
        Object resp = new Object();

        RpcService<Object, Object> healthy = immediateSuccess(resp);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        assertTrue(RoundRobin.isHealthy());
        assertEquals(RoundRobin.apply(req).get(), resp, "Response must match what is sent by backend");
        assertTrue(RoundRobin.isHealthy());
    }

    @Test
    public void testOneBackendSuccessAsync() throws ExecutionException, InterruptedException {
        Object req = new Object();
        Object resp = new Object();

        RpcService<Object, Object> healthy = immediateSuccess(resp);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        assertTrue(RoundRobin.isHealthy());
        ListenableFuture<Object> future = RoundRobin.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        wait.acquire();
        assertEquals(future.get(), resp, "Response must match what is sent by backend");
        assertTrue(RoundRobin.isHealthy());
    }

    @Test(expectedExceptions = CancellationException.class)
    public void testOneBackendFailBlocking() throws ExecutionException, InterruptedException {
        Object req = new Object();

        RpcService<Object, Object> healthy = immediateFail();

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        assertTrue(RoundRobin.isHealthy());
        RoundRobin.apply(req).get();
        fail();
    }

    @Test
    public void testOneBackendFailAsync() throws ExecutionException, InterruptedException {
        Object req = new Object();

        RpcService<Object, Object> healthy = immediateFail();

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        assertTrue(RoundRobin.isHealthy());
        ListenableFuture<Object> future = RoundRobin.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        wait.acquire();
        assertTrue(RoundRobin.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendServerDelayedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        assertTrue(RoundRobin.isHealthy());
        ListenableFuture<Object> future = RoundRobin.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        serverFuture.cancel(true);
        wait.acquire();
        assertTrue(RoundRobin.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendServerTimedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        assertTrue(RoundRobin.isHealthy());
        ListenableFuture<Object> future = RoundRobin.apply(req);
        future.addListener(wait::release, MoreExecutors.directExecutor());

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(() -> serverFuture.cancel(true), 1, TimeUnit.SECONDS);

        wait.acquire();
        assertTrue(RoundRobin.isHealthy());
        assertTrue(future.isCancelled(), "Client future must be in cancelled state");
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testOneBackendClientCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        ListenableFuture<Object> future = RoundRobin.apply(req);
        future.cancel(true);

        serverFuture.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();

        assertTrue(RoundRobin.isHealthy());
        assertTrue(serverFuture.isCancelled(), "Client future must be in cancelled state");
    }

    @Test
    public void testOneBackendClientDelayedCancel() throws ExecutionException, InterruptedException {
        Object req = new Object();

        SettableFuture<Object> serverFuture = SettableFuture.create();
        RpcService<Object, Object> healthy = custom(serverFuture);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(healthy));

        Semaphore wait = new Semaphore(0);

        ListenableFuture<Object> future = RoundRobin.apply(req);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.schedule(() -> future.cancel(true), 1, TimeUnit.SECONDS);


        serverFuture.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();

        assertTrue(RoundRobin.isHealthy());
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

        RoundRobin<Object, AtomicInteger> RoundRobin = new RoundRobin<>(
                ImmutableList.of(backend1, backend2, backend3));

        assertTrue(RoundRobin.isHealthy());
        assertEquals(RoundRobin.apply(req).get().incrementAndGet(), 1);
        assertEquals(RoundRobin.apply(req).get().incrementAndGet(), 1);
        assertEquals(RoundRobin.apply(req).get().incrementAndGet(), 1);
        assertEquals(resp1.get() + resp2.get() + resp3.get(), 3);
    }

    @Test
    public void testRoundRobinBehaviourForPoissonProcess() throws ExecutionException, InterruptedException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        long delay1 = 10;
        long delay2 = 20;
        long delay3 = 30;

        FakeBackend backend1 = new FakeBackend(scheduler, delay1);
        FakeBackend backend2 = new FakeBackend(scheduler, delay2);
        FakeBackend backend3 = new FakeBackend(scheduler, delay3);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(
                ImmutableList.<RpcService<Object, Object>>of(backend1, backend2, backend3));

        assertTrue(RoundRobin.isHealthy());

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
            scheduler.schedule(() -> RoundRobin.apply(new Object())
                            .addListener(countDownLatch::countDown, MoreExecutors.directExecutor()),
                    arrivalRates[i], TimeUnit.NANOSECONDS);
        }
        countDownLatch.await();

        double achievedRate = numRequests * 1e9 / watch.elapsed(TimeUnit.NANOSECONDS);
        System.err.println("RoundRobin: Throughput (Maximum = " + rate +")" +
                " (Actual = " + achievedRate + ")" );
        System.err.println("Backend (1: " + backend1.getNumRequests() + ")" +
                " (2: " + backend2.getNumRequests() + ")" +
                " (3: " + backend3.getNumRequests() + ")");
        assertTrue(Math.abs(backend1.getNumRequests() - backend2.getNumRequests()) < 3,
                "Backend 1 and Backend 2 must process similar amount of requests");
        assertTrue(Math.abs(backend2.getNumRequests() - backend3.getNumRequests()) < 3,
                "Backend 2 and Backend 3 must process similar amount of requests");
        assertTrue(Math.abs(backend1.getNumRequests() - backend3.getNumRequests()) < 3,
                "Backend 1 and Backend 3 must process similar amount of requests");
        scheduler.shutdown();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testFailedBehaviour() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(backend1));

        RoundRobin.apply(new Object()).get();
        fail();
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void testFailedBehaviourAsync() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(backend1));

        ListenableFuture<Object> future = RoundRobin.apply(new Object());
        Semaphore wait = new Semaphore(0);
        future.addListener(wait::release, MoreExecutors.directExecutor());
        wait.acquire();
        assertTrue(future.isDone());
        future.get();
        fail();
    }

    @Test
    public void testFailedRoundRobinBehaviour() throws ExecutionException, InterruptedException {

        RpcService<Object, Object> backend1 = custom(Futures.immediateFailedFuture(new RpcException("Test")));
        Object resp = new Object();
        RpcService<Object, Object> backend2 = immediateSuccess(resp);

        RoundRobin<Object, Object> RoundRobin = new RoundRobin<>(ImmutableList.of(backend1, backend2));

        while (true) {
            try {
                assertEquals(RoundRobin.apply(new Object()).get(), resp, "We should eventually get right response");
                return;
            } catch (ExecutionException ignored) {
            }
        }
    }

}
