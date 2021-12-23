/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper.oteltracegroup.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TraceGroup {
    public static final String TRACE_GROUP_NAME_FIELD = "traceGroup";
    public static final String TRACE_GROUP_END_TIME_FIELD = "traceGroupFields.endTime";
    public static final String TRACE_GROUP_STATUS_CODE_FIELD = "traceGroupFields.statusCode";
    public static final String TRACE_GROUP_DURATION_IN_NANOS_FIELD = "traceGroupFields.durationInNanos";

    @JsonProperty(TRACE_GROUP_NAME_FIELD)
    private final String name;

    @JsonProperty(TRACE_GROUP_END_TIME_FIELD)
    private final String endTime;

    @JsonProperty(TRACE_GROUP_DURATION_IN_NANOS_FIELD)
    private final Long durationInNanos;

    @JsonProperty(TRACE_GROUP_STATUS_CODE_FIELD)
    private final Integer statusCode;

    public String getName() {
        return name;
    }

    public String getEndTime() {
        return endTime;
    }

    public Long getDurationInNanos() {
        return durationInNanos;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public TraceGroup(final String name, final String endTime, final Long durationInNanos, final Integer statusCode) {
        this.name = name;
        this.endTime = endTime;
        this.durationInNanos = durationInNanos;
        this.statusCode = statusCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TraceGroup that = (TraceGroup) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(endTime, that.endTime) &&
                Objects.equals(durationInNanos, that.durationInNanos) &&
                Objects.equals(statusCode, that.statusCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, endTime, durationInNanos, statusCode);
    }
}
