/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class CreateScrollRequest {

    private final String index;
    private final String scrollTime;
    private final Integer size;

    public String getIndex() {
        return index;
    }

    public Integer getSize() { return size; }

    public String getScrollTime() { return scrollTime; }

    private CreateScrollRequest(final CreateScrollRequest.Builder builder) {
        this.index = builder.index;
        this.size = builder.size;
        this.scrollTime = builder.scrollTime;
    }

    public static CreateScrollRequest.Builder builder() {
        return new CreateScrollRequest.Builder();
    }

    public static class Builder {

        private String index;
        private Integer size;
        private String scrollTime;

        public Builder() {

        }

        public CreateScrollRequest.Builder withSize(final Integer size) {
            this.size = size;
            return this;
        }

        public CreateScrollRequest.Builder withIndex(final String index) {
            this.index = index;
            return this;
        }

        public CreateScrollRequest.Builder withScrollTime(final String scrollTime) {
            this.scrollTime = scrollTime;
            return this;
        }

        public CreateScrollRequest build() {
            return new CreateScrollRequest(this);
        }
    }
}
