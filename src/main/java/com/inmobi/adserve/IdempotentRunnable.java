package com.inmobi.adserve;

import java.util.concurrent.atomic.AtomicBoolean;

public class IdempotentRunnable implements Runnable {

    private final Runnable internal;

    private final AtomicBoolean done = new AtomicBoolean(false);

    private IdempotentRunnable(Runnable internal) {
        this.internal = internal;
    }

    @Override
    public void run() {
        if (done.compareAndSet(false, true)) {
            internal.run();
        }
    }

    public static IdempotentRunnable from(Runnable runnable) {
        if (runnable instanceof IdempotentRunnable) {
            return (IdempotentRunnable) runnable;
        } else {
            return new IdempotentRunnable(runnable);
        }
    }
}
