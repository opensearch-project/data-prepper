/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.lambda.sink.dlq;

import java.util.Objects;

public class LambdaSinkFailedDlqData {

    private final String functionName;
    private final int status;
    private final String message;
    private final Object data;

    private LambdaSinkFailedDlqData(final String functionName, final int status, final String message, final Object data) {
        Objects.requireNonNull(functionName);
        this.functionName = functionName;
        this.status = status;
        Objects.requireNonNull(message);
        this.message = message;
        Objects.requireNonNull(data);
        this.data = data;
    }

    public String getFunctionName() {
        return functionName;
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
        final LambdaSinkFailedDlqData that = (LambdaSinkFailedDlqData) o;
        return Objects.equals(functionName, that.functionName) &&
            Objects.equals(status, that.status) &&
            Objects.equals(message, that.message) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionName, status, message, data);
    }

    @Override
    public String toString() {
        return "LambdaSinkFailedDlqData{" +
            "functionName='" + functionName + '\'' +
            ", status='" + status + '\'' +
            ", message='" + message + '\'' +
            ", data=" + data +
            '}';
    }

    public static LambdaSinkFailedDlqData.Builder builder() {
        return new LambdaSinkFailedDlqData.Builder();
    }

    public static class Builder {

        private String functionName;
        private int status = 0;
        private String message;
        private Object data;

        public Builder withFunctionName(final String functionName) {
            this.functionName = functionName;
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

        public LambdaSinkFailedDlqData build() {
            return new LambdaSinkFailedDlqData(functionName, status, message, data);
        }
    }
}
