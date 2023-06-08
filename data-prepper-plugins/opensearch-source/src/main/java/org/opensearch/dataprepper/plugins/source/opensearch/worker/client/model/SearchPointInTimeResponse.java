/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public class SearchPointInTimeResponse {

    private final List<Event> documents;
    private final List<String> nextSearchAfter;
   
    public List<Event> getDocuments() {
        return documents;
    }

    public List<String> getNextSearchAfter() {
        return nextSearchAfter;
    }

    private SearchPointInTimeResponse(final SearchPointInTimeResponse.Builder builder) {
        this.documents = builder.documents;
        this.nextSearchAfter = builder.nextSearchAfter;
    }

    public static SearchPointInTimeResponse.Builder builder() {
        return new SearchPointInTimeResponse.Builder();
    }

    public static class Builder {

        private List<Event> documents;
        private List<String> nextSearchAfter;

        public Builder() {

        }

        public SearchPointInTimeResponse.Builder withDocuments(final List<Event> documents) {
            this.documents = documents;
            return this;
        }

        public SearchPointInTimeResponse.Builder withNextSearchAfter(final List<String> nextSearchAfter) {
            this.nextSearchAfter = nextSearchAfter;
            return this;
        }


        public SearchPointInTimeResponse build() {
            return new SearchPointInTimeResponse(this);
        }
    }
}
