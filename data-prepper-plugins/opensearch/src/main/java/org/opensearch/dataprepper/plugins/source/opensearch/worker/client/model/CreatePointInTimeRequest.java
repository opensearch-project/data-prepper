/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class CreatePointInTimeRequest {
    
    private final String index;
    private final String keepAlive;

    public String getIndex() {
        return index;
    }

    public String getKeepAlive() { return keepAlive; }
    
    private CreatePointInTimeRequest(final Builder builder) {
        this.index = builder.index;
        this.keepAlive = builder.keepAlive;
    }

    public static CreatePointInTimeRequest.Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String index;
        private String keepAlive;

        public Builder() {

        }

        public CreatePointInTimeRequest.Builder withIndex(final String index) {
            this.index = index;
            return this;
        }

        public CreatePointInTimeRequest.Builder withKeepAlive(final String keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public CreatePointInTimeRequest build() {
            return new CreatePointInTimeRequest(this);
        }
    }
}
