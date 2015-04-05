package com.inmobi.adserve;


import com.google.common.util.concurrent.ListenableFuture;

import java.util.function.Function;

/**
 * Abstracts out an RPC service. Which takes in a request object, and returns a promise of
 * response, which may be claimed when it is done, either by blocking or by attaching
 * a listener to it.
 *
 * @param <Req> Request object type.
 * @param <Resp> Response object type.
 */
public interface RpcService<Req, Resp> extends Function<Req, ListenableFuture<Resp>> {

    boolean isHealthy();
}
