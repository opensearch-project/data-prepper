/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Duration;
import java.time.Instant;

public class LatencyMetrics {
    public static final String INTERNAL_LATENCY = "internalLatency";
    public static final String EXTERNAL_LATENCY = "externalLatency";
    private final DistributionSummary internalLatencySummary;
    private final DistributionSummary externalLatencySummary;

    public LatencyMetrics(PluginMetrics pluginMetrics) {
        internalLatencySummary = pluginMetrics.summary(INTERNAL_LATENCY);
        externalLatencySummary = pluginMetrics.summary(EXTERNAL_LATENCY);
    }
    public void update(final EventHandle eventHandle) {
        if (eventHandle == null) {
            return;
        }
        Instant now = Instant.now();
        internalLatencySummary.record(Duration.between(eventHandle.getInternalOriginationTime(), now).toMillis());
        if (eventHandle.getExternalOriginationTime() == null) {
            return;
        }
        externalLatencySummary.record(Duration.between(eventHandle.getExternalOriginationTime(), now).toMillis());
    }
}
