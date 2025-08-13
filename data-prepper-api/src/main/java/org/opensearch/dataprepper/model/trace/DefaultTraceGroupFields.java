/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * The default implementation of {@link TraceGroupFields}, the attributes associated with an entire trace.
 *
 * @since 1.2
 */
public class DefaultTraceGroupFields implements TraceGroupFields, Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private String endTime;
    private Long durationInNanos;
    private Integer statusCode;

    // required for serialization
    DefaultTraceGroupFields() {}

    DefaultTraceGroupFields(final String endTime, final Long durationInNanos, final Integer statusCode) {
        this.endTime = endTime;
        this.durationInNanos = durationInNanos;
        this.statusCode = statusCode;
    }

    private DefaultTraceGroupFields(final Builder builder) {

        this.endTime = builder.endTime;
        this.durationInNanos = builder.durationInNanos;
        this.statusCode = builder.statusCode;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        String json = OBJECT_MAPPER.writeValueAsString(this);
        out.writeUTF(json);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        String json = in.readUTF();
        DefaultTraceGroupFields temp = OBJECT_MAPPER.readValue(json, DefaultTraceGroupFields.class);
        this.endTime = temp.endTime;
        this.durationInNanos = temp.durationInNanos;
        this.statusCode = temp.statusCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultTraceGroupFields that = (DefaultTraceGroupFields) o;
        return Objects.equals(endTime, that.endTime) && Objects.equals(durationInNanos, that.durationInNanos) && Objects.equals(statusCode, that.statusCode);
    }

    /*
    @Override
    public String toString() {
        return "DefaultTraceGroupFields{" +
                "endTime=" + endTime +
                ", durationInNanos=" + durationInNanos +
                ", statusCode='" + statusCode + '\'' +
                '}';
    }
    */

    @Override
    public String getEndTime() {
        return endTime;
    }

    @Override
    public Long getDurationInNanos() {
        return durationInNanos;
    }

    @Override
    public Integer getStatusCode() {
        return statusCode;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for creating {@link DefaultTraceGroupFields}
     * @since 1.2
     */
    public static class Builder {

        private String endTime;
        private Long durationInNanos;
        private Integer statusCode;

        public Builder withEndTime(final String endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder withDurationInNanos(final Long durationInNanos) {
            this.durationInNanos = durationInNanos;
            return this;
        }

        public Builder withStatusCode(final Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        /**
         * Returns a newly created {@link DefaultTraceGroupFields}.
         * @return a DefaultTraceGroupFields
         * @since 1.2
         */
        public DefaultTraceGroupFields build() {
            return new DefaultTraceGroupFields(this);
        }
    }
}
