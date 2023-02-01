/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

class SinkThread implements Runnable {
    private final AbstractSink sink;
    private int maxRetries;

    public SinkThread(AbstractSink sink, int maxRetries) {
        this.sink = sink;
        this.maxRetries = maxRetries;
    }

    @Override
    public void run() {
        int numRetries = 0;
        while (!sink.isReady() && numRetries++ < maxRetries) {
            try {
                Thread.sleep(1000);
                sink.doInitialize();
            } catch (Exception e){}
        }
    }
}
