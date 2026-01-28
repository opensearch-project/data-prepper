/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model.internal;

import org.opensearch.dataprepper.model.metric.Exemplar;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric aggregation state for in-memory collection during SERVER span processing
 */
public class MetricAggregationState {
    public long requestCount = 0;
    public long errorCount = 0;
    public long faultCount = 0;
    public final List<Exemplar> errorExemplars = new ArrayList<>();  // capped at 10
    public final List<Exemplar> faultExemplars = new ArrayList<>();  // capped at 10
    public final List<Double> latencyDurations = new ArrayList<>(); // durations in seconds for histogram
}
