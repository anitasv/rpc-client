package com.inmobi.adserve;


import com.google.common.util.concurrent.ListenableFuture;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Abstracts out an RPC service. Which takes in an object, and returns a promise of
 * response, which may be claimed when it is done, either by blocking or by attaching
 * a listener to it.
 *
 * @param <Req> Request object type.
 * @param <Resp> Response object type.
 */
@RequiredArgsConstructor
@Getter
public class RpcService<Req, Resp> {

    private final Function<Req, ListenableFuture<Resp>> service;

    private final Supplier<Boolean> healthInspector;
}
