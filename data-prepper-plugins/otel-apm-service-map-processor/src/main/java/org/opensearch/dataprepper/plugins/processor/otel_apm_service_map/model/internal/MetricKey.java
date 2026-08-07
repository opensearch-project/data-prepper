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

import lombok.Getter;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metric key for grouping spans by labels and time boundary
 */
@Getter
public class MetricKey {
    private final Map<String, Object> labels;
    private final Instant timestamp;

    public MetricKey(final Map<String, Object> labels, final Instant timestamp) {
        this.labels = Collections.unmodifiableMap(new HashMap<>(labels));
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricKey metricKey = (MetricKey) o;
        return Objects.equals(labels, metricKey.labels) &&
                Objects.equals(timestamp, metricKey.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, timestamp);
    }

    @Override
    public String toString() {
        return "MetricKey{" +
                "labels=" + labels +
                ", timestamp=" + timestamp +
                '}';
    }
}
