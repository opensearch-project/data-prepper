/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

public class SinkResponseLatency {
    
    final Duration latencyThreshold;
    final int numberThreshold;
    int numHighLatencyResponses;
    Backoff backoff;

    public SinkResponseLatency(Duration latencyThreshold, int numberThreshold) {
        this.latencyThreshold = latencyThreshold;
        this.numberThreshold = numberThreshold;
        this.backoff = null;
        this.numHighLatencyResponses = 0;
    }
    public void update(Duration latency) {
        if (latency > latencyThreshold) {
            numHighLatencyResponses++;
        } else if (numHighLatencyResponses > 0) {
            numHighLatencyResponses--;
        }
        if (numHighLatencyResponses > numberThreshold) {
            if (backoff == null) {
                backoff = Backoff.exponential(INITIAL_DELAY_MS, MAXIMUM_DELAY_MS);
            }
        } else if (numHighLatencyResponses < numberThreshold) {
            backoff = null;
        }
    }

    public void applyBackoffDelay() {
        if (backoff != null) {
            final long delayMillis = backoff.nextDelayMillis(attempt++);
            try {
                Thread.sleep(delayMillis);
            } catch (Exception e) {}
        }
    }
    
}

