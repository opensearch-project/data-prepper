/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import java.time.Duration;
import java.util.function.Consumer;

public class SinkResponseLatency {
    
    final Duration latencyThreshold;
    final int highLatencyResponsesThreshold;
    int numHighLatencyResponses;
    final Consumer<Object> callback;

    public SinkResponseLatency(Consumer<Object> callback, Duration latencyThreshold, int highLatencyResponsesThreshold) {
        this.latencyThreshold = latencyThreshold;
        this.highLatencyResponsesThreshold = highLatencyResponsesThreshold;
        this.numHighLatencyResponses = 0;
        this.callback = callback;
    }
    public void update(long latencyMs) {
        if (latencyMs > latencyThreshold.toMillis()) {
            numHighLatencyResponses++;
        } else if (numHighLatencyResponses > 0) {
            numHighLatencyResponses--;
        }
        if (numHighLatencyResponses > highLatencyResponsesThreshold) {
            callback.accept((double)numHighLatencyResponses/highLatencyResponsesThreshold);
        }
    }

}

