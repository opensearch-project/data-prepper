/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

public class SearchScrollRequest {

    private final String scrollId;
    private final String scrollTime;

    public String getScrollId() {
        return scrollId;
    }

    public String getScrollTime() { return scrollTime; }

    private SearchScrollRequest(final SearchScrollRequest.Builder builder) {
        this.scrollId = builder.scrollId;
        this.scrollTime = builder.scrollTime;
    }

    public static SearchScrollRequest.Builder builder() {
        return new SearchScrollRequest.Builder();
    }

    public static class Builder {

        private String scrollId;
        private String scrollTime;

        public Builder() {

        }

        public SearchScrollRequest.Builder withScrollId(final String scrollId) {
            this.scrollId = scrollId;
            return this;
        }

        public SearchScrollRequest.Builder withScrollTime(final String scrollTime) {
            this.scrollTime = scrollTime;
            return this;
        }

        public SearchScrollRequest build() {
            return new SearchScrollRequest(this);
        }
    }
}
