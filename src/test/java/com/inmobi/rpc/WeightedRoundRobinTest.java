package com.inmobi.rpc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;

public class WeightedRoundRobinTest {

    @Test
    public void testRoundRobinBehaviourForPoissonProcess() throws ExecutionException, InterruptedException {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        long delay1 = 10;
        long delay2 = 20;
        long delay3 = 30;

        FakeBackend backend1 = new FakeBackend(scheduler, delay1);
        FakeBackend backend2 = new FakeBackend(scheduler, delay2);
        FakeBackend backend3 = new FakeBackend(scheduler, delay3);

        WeightedRoundRobin<Object, Object> weightedRoundRobin = new WeightedRoundRobin<>(
                ImmutableList.<RpcService<Object, Object>>of(backend1, backend2, backend3),
                ImmutableList.of(3, 2, 1));

        assertTrue(weightedRoundRobin.isHealthy());

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
            scheduler.schedule(() -> weightedRoundRobin.apply(new Object())
                            .addListener(countDownLatch::countDown, MoreExecutors.directExecutor()),
                    arrivalRates[i], TimeUnit.NANOSECONDS);
        }
        countDownLatch.await();

        double achievedRate = numRequests * 1e9 / watch.elapsed(TimeUnit.NANOSECONDS);
        System.err.println("WeightedRoundRobin: Throughput (Maximum = " + rate +")" +
                " (Actual = " + achievedRate + ")" );
        System.err.println("Backend (1: " + backend1.getNumRequests() + ")" +
                " (2: " + backend2.getNumRequests() + ")" +
                " (3: " + backend3.getNumRequests() + ")");
        assertTrue(backend1.getNumRequests() > backend2.getNumRequests(),
                "Backend 1 must process more than Backend 2");
        assertTrue(backend2.getNumRequests() > backend3.getNumRequests(),
                "Backend 2 must process more than Backend 3");
        scheduler.shutdown();
    }
}
