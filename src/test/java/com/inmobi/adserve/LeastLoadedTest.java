package com.inmobi.adserve;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class LeastLoadedTest {


    public static <Req, Resp> RpcService<Req, Resp> immediateSuccess(Resp value) {
        return new RpcService<>(req -> Futures.immediateFuture(value), () -> true);
    }

    public static <Req, Resp> RpcService<Req, Resp> immediateFail() {
        return new RpcService<>(req -> Futures.immediateCancelledFuture(), () -> true);
    }

    public static <Req, Resp> RpcService<Req, Resp> custom(ListenableFuture future) {
        return new RpcService<>(req -> future, () -> true);
    }

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

        SettableFuture serverFuture = SettableFuture.create();
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

        SettableFuture serverFuture = SettableFuture.create();
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

        SettableFuture serverFuture = SettableFuture.create();
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
    public void testLeastLoadedBehaviour() throws ExecutionException, InterruptedException {
        Object req = new Object();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
        ScheduledExecutorService executorClient = Executors.newScheduledThreadPool(1);

        AtomicInteger counter1 = new AtomicInteger(0);
        AtomicInteger counter2 = new AtomicInteger(0);
        AtomicInteger counter3 = new AtomicInteger(0);

        long delay1 = 10;
        long delay2 = 20;
        long delay3 = 30;


        RpcService<Object, Object> backend1 = new RpcService<>(object -> {
            SettableFuture<Object> ret = SettableFuture.create();
            executor.schedule(() -> {
                ret.set(new Object());
                counter1.incrementAndGet();
            }, delay1, TimeUnit.MILLISECONDS);
            return ret;
        }, () -> true);
        RpcService<Object, Object> backend2 = new RpcService<>(object -> {
            SettableFuture<Object> ret = SettableFuture.create();
            executor.schedule(() -> {
                ret.set(new Object());
                counter2.incrementAndGet();
            }, delay2, TimeUnit.MILLISECONDS);
            return ret;
        }, () -> true);
        RpcService<Object, Object> backend3 = new RpcService<>(object -> {
            SettableFuture<Object> ret = SettableFuture.create();
            executor.schedule(() -> {
                ret.set(new Object());
                counter3.incrementAndGet();
            }, delay3, TimeUnit.MILLISECONDS);
            return ret;
        }, () -> true);

        LeastLoaded<Object, Object> leastLoaded = new LeastLoaded<>(
                ImmutableList.of(backend1, backend2, backend3),
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

        for (int i = 0; i < numRequests; i++) {
            executor.schedule(() -> {
                leastLoaded.apply(new Object())
                        .addListener(countDownLatch::countDown, MoreExecutors.directExecutor());
            }, arrivalRates[i], TimeUnit.NANOSECONDS);
        }
        countDownLatch.await();
        System.err.println(duration / delay1);
        System.err.println(duration / delay2);
        System.err.println(duration / delay3);
        System.err.println(counter1);
        System.err.println(counter2);
        System.err.println(counter3);

        assertTrue(counter1.get() > counter2.get(), "Backend 1 must process more requests than backend 2");
        assertTrue(counter3.get() > counter3.get(), "Backend 2 must process more requests than backend 3");
        executor.shutdown();
        executorClient.shutdown();
   }
}
