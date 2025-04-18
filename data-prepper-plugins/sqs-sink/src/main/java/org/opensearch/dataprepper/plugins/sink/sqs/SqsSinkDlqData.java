/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import java.util.Objects;

public class SqsSinkDlqData {
    private final String message;
    private final Object data;

    private SqsSinkDlqData(final String message, final Object data) {
        Objects.requireNonNull(message);
        this.message = message;
        Objects.requireNonNull(data);
        this.data = data;
    }

    public String getMessage() {
        return message;
    }

    public Object getData() {
        return data;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SqsSinkDlqData that = (SqsSinkDlqData) o;
        return Objects.equals(message, that.message) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, data);
    }

    @Override
    public String toString() {
        return "SqsSinkDlqData{" +
            ", message='" + message + '\'' +
            ", data=" + data +
            '}';
    }

    public static SqsSinkDlqData createDlqData(final Object data, final String failureMessage) {
        return SqsSinkDlqData.builder()
                .withData(data)
                .withMessage(failureMessage)
                .build();
    }

    public static SqsSinkDlqData.Builder builder() {
        return new SqsSinkDlqData.Builder();
    }

    public static class Builder {

        private String message;
        private Object data;

        public Builder withMessage(final String message) {
            this.message = message;
            return this;
        }

        public Builder withData(final Object data) {
            this.data = data;
            return this;
        }

        public SqsSinkDlqData build() {
            return new SqsSinkDlqData(message, data);
        }
    }
}
