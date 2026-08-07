/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import org.opensearch.dataprepper.model.metric.Exemplar;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric aggregation state for in-memory collection during SERVER span processing
 */
@Getter
public class MetricAggregationState {
    private long requestCount = 0;
    private long errorCount = 0;
    private long faultCount = 0;
    private final List<Exemplar> errorExemplars = new ArrayList<>();  // capped at 10
    private final List<Exemplar> faultExemplars = new ArrayList<>();  // capped at 10
    private final List<Double> latencyDurations = new ArrayList<>(); // durations in seconds for histogram

    public MetricAggregationState() {
        this(0L, 0L, 0L);
    }

    public MetricAggregationState(long requestCount, long errorCount, long faultCount) {
        this.requestCount = requestCount;
        this.errorCount = errorCount;
        this.faultCount = faultCount;
    }

    public void incrementRequestCount(final long value) {
        requestCount += value;
    }

    public void incrementErrorCount(final long value) {
        errorCount += value;
    }

    public void incrementFaultCount(final long value) {
        faultCount += value;
    }

    public void addErrorExemplar(final Exemplar exemplar) {
        errorExemplars.add(exemplar);
    }

    public void addFaultExemplar(final Exemplar exemplar) {
        faultExemplars.add(exemplar);
    }

    public void addLatencyDuration(final Double latency) {
        latencyDurations.add(latency);
    }

}
