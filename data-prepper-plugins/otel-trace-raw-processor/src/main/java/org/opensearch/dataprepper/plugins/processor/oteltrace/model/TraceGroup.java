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

    private TraceGroup(final String traceGroup, final TraceGroupFields traceGroupFields) {
        this.traceGroup = traceGroup;
        this.traceGroupFields = traceGroupFields;
    }

    public String getTraceGroup() {
        return traceGroup;
    }

    public TraceGroupFields getTraceGroupFields() {
        return traceGroupFields;
    }

    public static TraceGroup fromSpan(final Span span) {
        return new TraceGroup(span.getTraceGroup(), span.getTraceGroupFields());
    }
}
