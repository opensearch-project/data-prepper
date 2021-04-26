package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.opentelemetry.proto.trace.v1.Span;

import java.util.Objects;

public class TraceGroup {
    private final String name;
    private final String endTime;
    private final Integer statusCode;
    private final Long durationInNanos;

    public String getName() {
        return name;
    }

    public String getEndTime() {
        return endTime;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public Long getDurationInNanos() {
        return durationInNanos;
    }

    TraceGroup(TraceGroupBuilder traceGroupBuilder) {
        name = traceGroupBuilder.name;
        endTime = traceGroupBuilder.endTime;
        statusCode = traceGroupBuilder.statusCode;
        durationInNanos = traceGroupBuilder.durationInNanos;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TraceGroup that = (TraceGroup) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(endTime, that.endTime) &&
                Objects.equals(statusCode, that.statusCode) &&
                Objects.equals(durationInNanos, that.durationInNanos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, endTime, statusCode, durationInNanos);
    }

    public static class TraceGroupBuilder {
        private String name;
        private String endTime;
        private Integer statusCode;
        private Long durationInNanos;

        private TraceGroupBuilder setName(final String name) {
            this.name = name;
            return this;
        }

        private TraceGroupBuilder setEndTime(final String endTime) {
            this.endTime = endTime;
            return this;
        }

        private TraceGroupBuilder setStatusCode(final Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        private TraceGroupBuilder setDurationInNanos(final Long durationInNanos) {
            this.durationInNanos = durationInNanos;
            return this;
        }

        public TraceGroup build() {
            return new TraceGroup(this);
        }

        public TraceGroupBuilder setFromSpan(final Span span) {
            return this
                    .setName(span.getName())
                    .setDurationInNanos(span.getEndTimeUnixNano() - span.getStartTimeUnixNano())
                    .setEndTime(OTelProtoHelper.getEndTimeISO8601(span))
                    .setStatusCode(span.getStatus().getCodeValue());
        }
    }
}
