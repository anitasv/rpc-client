package com.inmobi.adserve;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Submit multiple async tasks to the queue. Only when the previous async task
 * is completed next one is started. I've not proved this to be right, but it is
 * not critical, as this is just a test utility class.
 */
public class AsyncQueue {

    public static interface AsyncTask {

        void perform(Runnable completionHook);
    }

    private final AtomicBoolean free = new AtomicBoolean(true);

    private final Queue<AsyncTask> asyncTaskQueue = new ArrayDeque<>();

    public void submit(AsyncTask asyncTask) {
        synchronized (asyncTaskQueue) {
            asyncTaskQueue.add(asyncTask);
            performTriggers();
        }
    }

    private void performTriggers() {
        synchronized (asyncTaskQueue) {
            if (!asyncTaskQueue.isEmpty()) {
                if (free.compareAndSet(true, false)) {
                    asyncTaskQueue.poll().perform(
                            () -> {
                                free.set(true);
                                this.performTriggers();
                            }
                    );
                }
            }
        }
    }
}
