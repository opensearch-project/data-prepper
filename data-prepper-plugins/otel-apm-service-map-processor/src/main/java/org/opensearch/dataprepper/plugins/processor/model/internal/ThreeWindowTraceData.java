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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Data structure to hold three-window trace processing data
 */
public class ThreeWindowTraceData {
    public final Collection<SpanStateData> processingSpans;
    public final Collection<SpanStateData> lookupSpans;
    public final Map<String, SpanStateData> spansBySpanId;
    public final Map<String, Collection<SpanStateData>> childrenByParentId;
    public final Set<String> processingSpanIds;

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
