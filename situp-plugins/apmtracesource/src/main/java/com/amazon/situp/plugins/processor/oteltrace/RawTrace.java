package com.amazon.situp.plugins.processor.oteltrace;

import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;

public class RawTrace {

    private final String traceID;
    private final String spanID;
    private final String parentSpanID;
    private final String name;
    private final String startTime;
    private final String endTime;
    private final String instrumentationName;
    private final String instrumentationVersion;


    private RawTrace(RawTraceBuilder builder) {
        this.traceID = builder.traceID;
        this.spanID = builder.spanID;
        this.parentSpanID = builder.parentSpanID;
        this.name = builder.name;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.instrumentationName = builder.instrumentationName;
        this.instrumentationVersion = builder.instrumentationVersion;
    }

    /**
     * Builder class
     **/
    public static class RawTraceBuilder {
        private String traceID;
        private String spanID;
        private String parentSpanID;
        private String name;
        private String startTime;
        private String endTime;
        private String instrumentationName;
        private String instrumentationVersion;

        public RawTraceBuilder() {
        }

        public RawTraceBuilder setTraceID(String traceID) {
            this.traceID = traceID;
            return this;
        }

        public RawTraceBuilder setSpanID(String spanID) {
            this.spanID = spanID;
            return this;
        }

        public RawTraceBuilder setParentSpanID(String parentSpanID) {
            this.parentSpanID = parentSpanID;
            return this;
        }

        public RawTraceBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public RawTraceBuilder setStartTime(String startTime) {
            this.startTime = startTime;
            return this;
        }

        public RawTraceBuilder setEndTime(String endTime) {
            this.endTime = endTime;
            return this;
        }

        public RawTraceBuilder setInstrumentationName(String instrumentationName) {
            this.instrumentationName = instrumentationName;
            return this;
        }

        public RawTraceBuilder setInstrumentationVersion(String instrumentationVersion) {
            this.instrumentationVersion = instrumentationVersion;
            return this;
        }


        public RawTrace build() {
            return new RawTrace(this);
        }

    }

    public static RawTrace buildFromProto(Span spans, InstrumentationLibrary instrumentationLibrary) {
        return new RawTrace.RawTraceBuilder()
                .setTraceID(spans.getTraceId().toString())
                .setSpanID(spans.getSpanId().toString())
                .setParentSpanID(spans.getParentSpanId().toString())
                .setName(spans.getName())
                .setStartTime(String.valueOf(spans.getStartTimeUnixNano()))
                .setEndTime(String.valueOf(spans.getEndTimeUnixNano()))
                .setInstrumentationName(instrumentationLibrary.getName())
                .setInstrumentationVersion(instrumentationLibrary.getVersion())
                .build();
    }
}
