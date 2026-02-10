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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Data structure to hold three-window trace processing data
 */
@Getter
public class ThreeWindowTraceData {
    private final Collection<SpanStateData> processingSpans;
    private final Collection<SpanStateData> lookupSpans;
    private final Map<String, SpanStateData> spansBySpanId;
    private final Map<String, Collection<SpanStateData>> childrenByParentId;
    private final Set<String> processingSpanIds;

    public ThreeWindowTraceData(final Collection<SpanStateData> processingSpans,
                                final Collection<SpanStateData> lookupSpans,
                                final Map<String, SpanStateData> spansBySpanId,
                                final Map<String, Collection<SpanStateData>> childrenByParentId,
                                final Set<String> processingSpanIds) {
        this.processingSpans = processingSpans;
        this.lookupSpans = lookupSpans;
        this.spansBySpanId = spansBySpanId;
        this.childrenByParentId = childrenByParentId;
        this.processingSpanIds = processingSpanIds;
    }
}
