package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import java.util.Objects;

public class TraceGroupWrapper {
    private final String name;
    private final String endTime;
    private final Long durationInNanos;
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

    TraceGroupWrapper(final String name, final String endTime, final Long durationInNanos, final Integer statusCode) {
        this.name = name;
        this.endTime = endTime;
        this.durationInNanos = durationInNanos;
        this.statusCode = statusCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TraceGroupWrapper that = (TraceGroupWrapper) o;
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
