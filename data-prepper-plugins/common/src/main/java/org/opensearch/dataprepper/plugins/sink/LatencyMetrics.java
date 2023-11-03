/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.time.Duration;
import java.time.Instant;

public class LatencyMetrics {
    public static final String INTERNAL_LATENCY_MIN_MS = "internalLatencyMinMs";
    public static final String INTERNAL_LATENCY_MAX_MS = "internalLatencyMaxMs";
    public static final String INTERNAL_LATENCY_AVG_MS = "internalLatencyAverageMs";
    public static final String EXTERNAL_LATENCY_MIN_MS = "externalLatencyMinMs";
    public static final String EXTERNAL_LATENCY_MAX_MS = "externalLatencyMaxMs";
    public static final String EXTERNAL_LATENCY_AVG_MS = "externalLatencyAverageMs";
    private long internalLatencyEventCount;
    private long externalLatencyEventCount;
    private long internalLatencyMin;
    private long internalLatencyMax;
    private double internalLatencyAverage;
    private long externalLatencyMin;
    private long externalLatencyMax;
    private double externalLatencyAverage;
    private PluginMetrics pluginMetrics;

    public LatencyMetrics(PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.internalLatencyEventCount = 0L;
        this.externalLatencyEventCount = 0L;
        this.internalLatencyMin = Long.MAX_VALUE;
        this.internalLatencyMax = 0L;
        this.internalLatencyAverage = 0.0;
        this.externalLatencyMin = Long.MAX_VALUE;
        this.externalLatencyMax = 0L;
        this.externalLatencyAverage = 0.0;
    }
    public void update(final EventHandle eventHandle) {
        final long internalLatency = Duration.between(eventHandle.getInternalOriginationTime(), Instant.now()).toMillis();
        pluginMetrics.gauge(INTERNAL_LATENCY_MIN_MS, internalLatency, latency -> Math.min(internalLatencyMin, latency));
        pluginMetrics.gauge(INTERNAL_LATENCY_MAX_MS, internalLatency, latency -> Math.max(internalLatencyMax, latency));
        pluginMetrics.gauge(INTERNAL_LATENCY_AVG_MS, internalLatency, latency -> {
            internalLatencyAverage = (internalLatencyEventCount*internalLatencyAverage + latency)/(internalLatencyEventCount+1);
            return internalLatencyAverage;
        });
        internalLatencyEventCount++;
        if (eventHandle.getExternalOriginationTime() == null) {
            return;
        }
        final long externalLatency = Duration.between(eventHandle.getInternalOriginationTime(), Instant.now()).toMillis();
        pluginMetrics.gauge(EXTERNAL_LATENCY_MIN_MS, externalLatency, latency -> Math.min(externalLatencyMin, latency));
        pluginMetrics.gauge(EXTERNAL_LATENCY_MAX_MS, externalLatency, latency -> Math.max(externalLatencyMax, latency));
        externalLatencyAverage = (externalLatencyEventCount*externalLatencyAverage + externalLatency)/(externalLatencyEventCount+1);
        pluginMetrics.gauge(EXTERNAL_LATENCY_AVG_MS, externalLatency, latency -> {
            externalLatencyAverage = (externalLatencyEventCount*externalLatencyAverage + latency)/(externalLatencyEventCount+1);
            return externalLatencyAverage;
        });
    }
}
