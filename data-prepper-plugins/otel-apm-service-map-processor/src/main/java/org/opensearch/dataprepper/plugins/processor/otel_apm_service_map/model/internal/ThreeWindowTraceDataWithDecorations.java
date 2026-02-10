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
 * Extended trace data that includes ephemeral decorations.
 * This class extends ThreeWindowTraceData with ephemeral decoration storage
 * that exists only during the processing cycle.
 */
@Getter
public class ThreeWindowTraceDataWithDecorations extends ThreeWindowTraceData {
    private final EphemeralSpanDecorations decorations;

    /**
     * Constructor for three-window trace data with ephemeral decorations
     *
     * @param processingSpans Spans from current window being processed
     * @param lookupSpans All spans from three windows for relationship lookup
     * @param spansBySpanId Index of spans by their span ID
     * @param childrenByParentId Index of child spans by parent span ID
     * @param processingSpanIds Set of span IDs from processing spans
     * @param decorations Ephemeral decoration storage for this processing cycle
     */
    public ThreeWindowTraceDataWithDecorations(final Collection<SpanStateData> processingSpans,
                                              final Collection<SpanStateData> lookupSpans,
                                              final Map<String, SpanStateData> spansBySpanId,
                                              final Map<String, Collection<SpanStateData>> childrenByParentId,
                                              final Set<String> processingSpanIds,
                                              final EphemeralSpanDecorations decorations) {
        super(processingSpans, lookupSpans, spansBySpanId, childrenByParentId, processingSpanIds);
        this.decorations = decorations;
    }
}
