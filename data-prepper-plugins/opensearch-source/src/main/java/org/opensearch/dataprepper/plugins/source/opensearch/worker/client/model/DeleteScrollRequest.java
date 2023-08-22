/*
 * Copyright OpenDelete Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class DeleteScrollRequest {

    private final String scrollId;

    public String getScrollId() {
        return scrollId;
    }

    private DeleteScrollRequest(final DeleteScrollRequest.Builder builder) {
        this.scrollId = builder.scrollId;
    }

    public static DeleteScrollRequest.Builder builder() {
        return new DeleteScrollRequest.Builder();
    }

    public static class Builder {

        private String scrollId;

        public Builder() {

        }

        public DeleteScrollRequest.Builder withScrollId(final String scrollId) {
            this.scrollId = scrollId;
            return this;
        }

        public DeleteScrollRequest build() {
            return new DeleteScrollRequest(this);
        }
    }
}
