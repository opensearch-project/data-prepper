/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace.model;

import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;

public class TraceGroup {
    private final String traceGroup;

    private final TraceGroupFields traceGroupFields;

    private final String rootSpanId;

    private TraceGroup(final String traceGroup, final TraceGroupFields traceGroupFields, final String rootSpanId) {
        this.traceGroup = traceGroup;
        this.traceGroupFields = traceGroupFields;
        this.rootSpanId = rootSpanId;
    }

    public String getTraceGroup() {
        return traceGroup;
    }

    public TraceGroupFields getTraceGroupFields() {
        return traceGroupFields;
    }

    /**
     * The span id of the trace's root span, so a span's ancestor chain can be known to end at the
     * root without keeping state for traces that have no GenAI data.
     *
     * @return the root span id
     */
    public String getRootSpanId() {
        return rootSpanId;
    }

    public static TraceGroup fromSpan(final Span span) {
        return new TraceGroup(span.getTraceGroup(), span.getTraceGroupFields(), span.getSpanId());
    }
}
