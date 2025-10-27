/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.ml_inference.processor.dlq;

import java.util.Objects;

public class MLBatchJobFailedDlqData {

    private final String s3Bucket;
    private final String s3Key;
    private final int status;
    private final String message;
    private final Object data;

    private MLBatchJobFailedDlqData(final String s3Bucket, final String s3Key, final int status, final String message, final Object data) {
        Objects.requireNonNull(s3Bucket);
        this.s3Bucket = s3Bucket;
        Objects.requireNonNull(s3Key);
        this.s3Key = s3Key;
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        Objects.requireNonNull(data);
        this.data = data;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Key() { return s3Key; }

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
        final MLBatchJobFailedDlqData that = (MLBatchJobFailedDlqData) o;
        return Objects.equals(s3Bucket, that.s3Bucket) &&
                Objects.equals(s3Key, that.s3Key) &&
                Objects.equals(status, that.status) &&
                Objects.equals(message, that.message) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s3Bucket + s3Key, status, message, data);
    }

    @Override
    public String toString() {
        return "MLBatchJobFailedDlqData{" +
                "s3Bucket='" + s3Bucket + '\'' +
                ", s3Key='" + s3Key + '\'' +
                ", status='" + status + '\'' +
                ", message='" + message + '\'' +
                ", data=" + data +
                '}';
    }

    public static MLBatchJobFailedDlqData.Builder builder() {
        return new MLBatchJobFailedDlqData.Builder();
    }

    public static class Builder {

        private String s3Bucket;
        private String s3Key;
        private int status = 0;
        private String message;
        private Object data;

        public Builder withS3Bucket(final String s3Bucket) {
            this.s3Bucket = s3Bucket;
            return this;
        }

        public Builder withS3Key(final String s3Key) {
            this.s3Key = s3Key;
            return this;
        }

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

        public MLBatchJobFailedDlqData build() {
            return new MLBatchJobFailedDlqData(s3Bucket, s3Key, status, message, data);
        }
    }
}
