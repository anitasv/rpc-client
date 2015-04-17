package com.inmobi.rpc;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class RpcTestUtils {

    public static <Req, Resp> RpcService<Req, Resp> immediateSuccess(Resp value) {
        return new FunctionalRpcService<>(req -> Futures.immediateFuture(value), () -> true);
    }

    public static <Req, Resp> RpcService<Req, Resp> immediateFail() {
        return new FunctionalRpcService<>(req -> Futures.immediateCancelledFuture(), () -> true);
    }

    public static <Req, Resp> RpcService<Req, Resp> custom(ListenableFuture<Resp> future) {
        return new FunctionalRpcService<>(req -> future, () -> true);
    }
}
