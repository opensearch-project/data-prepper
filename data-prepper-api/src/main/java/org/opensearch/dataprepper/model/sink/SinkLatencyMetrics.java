/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Duration;
import java.time.Instant;

public class SinkLatencyMetrics {
    public static final String INTERNAL_LATENCY = "PipelineLatency";
    public static final String EXTERNAL_LATENCY = "EndToEndLatency";
    private final Timer internalLatencyTimer;
    private final Timer externalLatencyTimer;

    public SinkLatencyMetrics(PluginMetrics pluginMetrics) {
        internalLatencyTimer = pluginMetrics.timer(INTERNAL_LATENCY);
        externalLatencyTimer = pluginMetrics.timer(EXTERNAL_LATENCY);
    }
    public void update(final EventHandle eventHandle) {
        Instant now = Instant.now();
        internalLatencyTimer.record(Duration.between(eventHandle.getInternalOriginationTime(), now));
        if (eventHandle.getExternalOriginationTime() == null) {
            return;
        }
        externalLatencyTimer.record(Duration.between(eventHandle.getExternalOriginationTime(), now));
    }
}
