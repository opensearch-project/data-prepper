package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.model.trace.TraceGroupFields;

public class TraceGroup {
    private final String traceGroup;

    private final TraceGroupFields traceGroupFields;

    public String getTraceGroup() {
        return traceGroup;
    }

    public TraceGroupFields getTraceGroupFields() {
        return traceGroupFields;
    }

    TraceGroup(TraceGroupBuilder traceGroupBuilder) {
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
