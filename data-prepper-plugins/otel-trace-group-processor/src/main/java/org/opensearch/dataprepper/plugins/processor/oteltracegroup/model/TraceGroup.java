/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup.model;

import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;

import java.util.Objects;

public class TraceGroup {
    public static final String TRACE_GROUP_NAME_FIELD = "traceGroup";
    public static final String TRACE_GROUP_END_TIME_FIELD = "traceGroupFields.endTime";
    public static final String TRACE_GROUP_STATUS_CODE_FIELD = "traceGroupFields.statusCode";
    public static final String TRACE_GROUP_DURATION_IN_NANOS_FIELD = "traceGroupFields.durationInNanos";

    private final String traceGroup;

    private final TraceGroupFields traceGroupFields;

    public String getTraceGroup() {
        return traceGroup;
    }

    public TraceGroupFields getTraceGroupFields() {
        return traceGroupFields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceGroup that = (TraceGroup) o;
        return Objects.equals(traceGroup, that.traceGroup) && Objects.equals(traceGroupFields, that.traceGroupFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceGroup, traceGroupFields);
    }

    TraceGroup(final TraceGroupBuilder traceGroupBuilder) {
        traceGroup = traceGroupBuilder.traceGroup;
        traceGroupFields = traceGroupBuilder.traceGroupFields;
    }

    public static class TraceGroupBuilder {
        private String traceGroup;
        private TraceGroupFields traceGroupFields;

        public TraceGroupBuilder setTraceGroup(String traceGroup) {
            this.traceGroup = traceGroup;
            return this;
        }

        public TraceGroupBuilder setTraceGroupFields(TraceGroupFields traceGroupFields) {
            this.traceGroupFields = traceGroupFields;
            return this;
        }

        public TraceGroup build() {
            return new TraceGroup(this);
        }

        public TraceGroupBuilder setFromSpan(final Span span) {
            return this
                    .setTraceGroup(span.getTraceGroup())
                    .setTraceGroupFields(span.getTraceGroupFields());
        }
    }
}
