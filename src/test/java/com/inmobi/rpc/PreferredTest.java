package com.inmobi.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PreferredTest {

    @Test
    public void testOneHealthyBackend() throws ExecutionException, InterruptedException {

        Object resp1 = new Object();
        RpcService<Object, Object> backend1 = RpcTestUtils.immediateSuccess(resp1);

        Preferred<Object, Object> preferred = new Preferred<>(ImmutableList.of(backend1));

        for (int i = 0; i < 10; i++) {
            assertEquals(preferred.apply(new Object()).get(), resp1, "Must relay the response from single backend");
        }
    }

    @Test
    public void testTwoHealthyBackends() throws ExecutionException, InterruptedException {

        Object resp1 = new Object();
        Object resp2 = new Object();
        RpcService<Object, Object> backend1 = RpcTestUtils.immediateSuccess(resp1);
        RpcService<Object, Object> backend2 = RpcTestUtils.immediateSuccess(resp2);

        Preferred<Object, Object> preferred = new Preferred<>(ImmutableList.of(backend1, backend2));

        for (int i = 0; i < 10; i++) {
            assertEquals(preferred.apply(new Object()).get(), resp1, "Must relay the response from first backend");
        }
    }

    @Test
    public void testTenHealthyBackends() throws ExecutionException, InterruptedException {

        int numBackends = 10;

        Object[] responses = new Object[numBackends];
        for (int i = 0; i < numBackends; i++) {
            responses[i] = new Object();
        }
        ImmutableList.Builder<RpcService<Object, Object>> immutableListBuilder = ImmutableList.builder();
        for (int i = 0; i < numBackends; i++) {
            immutableListBuilder.add(RpcTestUtils.immediateSuccess(responses[i]));
        }

        Preferred<Object, Object> preferred = new Preferred<>(immutableListBuilder.build());

        for (int i = 0; i < 10; i++) {
            assertEquals(preferred.apply(new Object()).get(), responses[0], "Must relay the response from first backend");
        }
    }

    @Test(expectedExceptions = CancellationException.class)
    public void testTenHealthyBackendsFirstCancelBlocking() throws ExecutionException, InterruptedException {

        int numBackends = 10;

        ImmutableList.Builder<RpcService<Object, Object>> immutableListBuilder = ImmutableList.builder();
        immutableListBuilder.add(RpcTestUtils.immediateFail());
        for (int i = 0; i < numBackends - 1; i++) {
            immutableListBuilder.add(RpcTestUtils.immediateSuccess(new Object()));
        }

        Preferred<Object, Object> preferred = new Preferred<>(immutableListBuilder.build());

        preferred.apply(new Object()).get();
        fail("First server even though marked as healthy, it should cancel");
    }

    @Test
    public void testTenHealthyBackendsFirstFailAsync() throws ExecutionException, InterruptedException {

        int numBackends = 10;

        ImmutableList.Builder<RpcService<Object, Object>> immutableListBuilder = ImmutableList.builder();
        immutableListBuilder.add(RpcTestUtils.immediateFail());
        for (int i = 0; i < numBackends - 1; i++) {
            immutableListBuilder.add(RpcTestUtils.immediateSuccess(new Object()));
        }

        Preferred<Object, Object> preferred = new Preferred<>(immutableListBuilder.build());

        Semaphore semaphore = new Semaphore(0);
        ListenableFuture<Object> future = preferred.apply(new Object());
        future.addListener(semaphore::release, MoreExecutors.directExecutor());
        semaphore.acquire();

        assertTrue(future.isCancelled(), "First server even though marked as healthy, it should cancel");
    }

    @Test
    public void testFirstUnhealthyNineHealthyBackendsFirstFailAsync() throws ExecutionException, InterruptedException {

        int numBackends = 10;

        ImmutableList.Builder<RpcService<Object, Object>> immutableListBuilder = ImmutableList.builder();
        immutableListBuilder.add(new FunctionalRpcService<>((req) -> Futures.immediateCancelledFuture(), () -> false));
        for (int i = 0; i < numBackends - 1; i++) {
            immutableListBuilder.add(RpcTestUtils.immediateSuccess(new Object()));
        }

        Preferred<Object, Object> preferred = new Preferred<>(immutableListBuilder.build());

        Semaphore semaphore = new Semaphore(0);
        ListenableFuture<Object> future = preferred.apply(new Object());
        future.addListener(semaphore::release, MoreExecutors.directExecutor());
        semaphore.acquire();

        assertTrue(!future.isCancelled(), "First server marked as unhealthy, it shouldn't be called");
    }
}
