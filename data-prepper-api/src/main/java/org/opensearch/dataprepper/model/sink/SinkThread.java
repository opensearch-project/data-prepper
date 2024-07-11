/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

class SinkThread implements Runnable {
    private final AbstractSink sink;
    private int maxRetries;
    private int waitTimeMs;

    private volatile boolean isStopped = false;

    public SinkThread(AbstractSink sink, int maxRetries, int waitTimeMs) {
        this.sink = sink;
        this.maxRetries = maxRetries;
        this.waitTimeMs = waitTimeMs;
    }

    @Override
    public void run() {
        int numRetries = 0;
        while (!sink.isReady() && numRetries++ < maxRetries && !isStopped) {
            try {
                Thread.sleep(waitTimeMs);
                sink.doInitialize();
            } catch (InterruptedException e){}
        }
    }

    public void stop() {
        isStopped = true;
    }
}
