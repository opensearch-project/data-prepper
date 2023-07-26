/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.dlq;

public class FailedDlqData {

    private String url;

    private int status;

    private String message;

    public FailedDlqData(final Builder builder) {
        this.status = builder.status;
        this.message = builder.message;
        this.url = builder.url;
    }

    public String getUrl() {
        return url;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String url;

        private int status;

        private String message;


        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withStatus(int status) {
            this.status = status;
            return this;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public FailedDlqData build() {
            return new FailedDlqData(this);
        }
    }
}
