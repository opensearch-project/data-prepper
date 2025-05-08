/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.dlq;

import java.util.Objects;

public class CloudWatchLogsSinkDlqData {
    private final int status;
    private final String message;
    private final Object data;

    private CloudWatchLogsSinkDlqData(final int status, final String message, final Object data) {
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        Objects.requireNonNull(data);
        this.data = data;
    }

    public int getStatus() {
        return status;
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
        final CloudWatchLogsSinkDlqData that = (CloudWatchLogsSinkDlqData) o;
        return Objects.equals(status, that.status) &&
            Objects.equals(message, that.message) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, message, data);
    }

    @Override
    public String toString() {
        return "CloudWatchLogsSinkDlqData{" +
            "status='" + status + '\'' +
            ", message='" + message + '\'' +
            ", data=" + data +
            '}';
    }

    public static CloudWatchLogsSinkDlqData createDlqData(final int status, final Object data, final String failureMessage) {
        return CloudWatchLogsSinkDlqData.builder()
                .withData(data)
                .withStatus(status)
                .withMessage(failureMessage)
                .build();
    }

    public static CloudWatchLogsSinkDlqData.Builder builder() {
        return new CloudWatchLogsSinkDlqData.Builder();
    }

    public static class Builder {

        private int status = 0;
        private String message;
        private Object data;

        public Builder withStatus(final int status) {
            this.status = status;
            return this;
        }

        public Builder withMessage(final String message) {
            this.message = message;
            return this;
        }

        public Builder withData(final Object data) {
            this.data = data;
            return this;
        }

        public CloudWatchLogsSinkDlqData build() {
            return new CloudWatchLogsSinkDlqData(status, message, data);
        }
    }
}
