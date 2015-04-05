package com.inmobi.adserve;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.AllArgsConstructor;

import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Easy to make implementation of RpcService, in functional style, see tests.
 *
 * @param <Req>
 * @param <Resp>
 */
@AllArgsConstructor
public class FunctionalRpcService<Req, Resp> implements RpcService<Req, Resp> {

    private final Function<Req, ListenableFuture<Resp>> service;
    private final BooleanSupplier healthInspector;

    @Override
    public ListenableFuture<Resp> apply(Req request) {
        return service.apply(request);
    }

    @Override
    public boolean isHealthy() {
        return healthInspector.getAsBoolean();
    }
}
