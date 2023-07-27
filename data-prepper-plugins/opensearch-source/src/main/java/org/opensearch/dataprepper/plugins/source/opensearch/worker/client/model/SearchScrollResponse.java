/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public class SearchScrollResponse {

    private final String scrollId;
    private final List<Event> documents;

    public String getScrollId() {
        return scrollId;
    }

    public List<Event> getDocuments() { return documents; }

    private SearchScrollResponse(final SearchScrollResponse.Builder builder) {
        this.scrollId = builder.scrollId;
        this.documents = builder.documents;
    }

    public static SearchScrollResponse.Builder builder() {
        return new SearchScrollResponse.Builder();
    }

    public static class Builder {

        private String scrollId;
        private List<Event> documents;

        public Builder() {

        }

        public SearchScrollResponse.Builder withScrollId(final String scrollId) {
            this.scrollId = scrollId;
            return this;
        }

        public SearchScrollResponse.Builder withDocuments(final List<Event> documents) {
            this.documents = documents;
            return this;
        }

        public SearchScrollResponse build() {
            return new SearchScrollResponse(this);
        }
    }
}
