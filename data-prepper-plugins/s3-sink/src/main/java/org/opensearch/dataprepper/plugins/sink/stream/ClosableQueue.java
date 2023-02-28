/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.stream;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClosableQueue<T> extends ArrayBlockingQueue<T> {
    private volatile boolean closed = false;

    public ClosableQueue(int capacity) {
        super(capacity);
    }

    public void close() {
        closed = true;
    }

    @Override
    public void put(T t) throws InterruptedException {
        while (!offer(t, 1, TimeUnit.SECONDS)) {
            if (closed) {
                throw new IllegalStateException(
                        "The queue is now closed due to an error elsewhere"
                );
            }
        }
    }
}
