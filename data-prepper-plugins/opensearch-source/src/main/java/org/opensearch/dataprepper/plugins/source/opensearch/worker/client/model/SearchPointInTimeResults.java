/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public class SearchPointInTimeResults {

    private final List<Event> documents;
    private final List<String> nextSearchAfter;
   
    public List<Event> getDocuments() {
        return documents;
    }

    public List<String> getNextSearchAfter() {
        return nextSearchAfter;
    }

    private SearchPointInTimeResults(final SearchPointInTimeResults.Builder builder) {
        this.documents = builder.documents;
        this.nextSearchAfter = builder.nextSearchAfter;
    }

    public static SearchPointInTimeResults.Builder builder() {
        return new SearchPointInTimeResults.Builder();
    }

    public static class Builder {

        private List<Event> documents;
        private List<String> nextSearchAfter;

        public Builder() {

        }

        public SearchPointInTimeResults.Builder withDocuments(final List<Event> documents) {
            this.documents = documents;
            return this;
        }

        public SearchPointInTimeResults.Builder withNextSearchAfter(final List<String> nextSearchAfter) {
            this.nextSearchAfter = nextSearchAfter;
            return this;
        }


        public SearchPointInTimeResults build() {
            return new SearchPointInTimeResults(this);
        }
    }
}
