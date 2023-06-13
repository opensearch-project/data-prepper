/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import java.util.List;

public class SearchPointInTimeRequest {

    private final String pitId;
    private final String keepAlive;
    private final String index;
    private final List<String> searchAfter;
    private final Integer paginationSize;
    private final String query;
    private final List<SortingOptions> sortingOptions;

    public String getIndex() {
        return index;
    }

    public List<SortingOptions> getSortOptions() {
        return sortingOptions;
    }

    public String getQuery() {
        return query;
    }

    public Integer getPaginationSize() {
        return paginationSize;
    }

    public List<String> getSearchAfter() {
        return searchAfter;
    }

    public String getPitId() {
        return pitId;
    }

    public String getKeepAlive() { return keepAlive; }

    private SearchPointInTimeRequest(final SearchPointInTimeRequest.Builder builder) {
        this.pitId = builder.pitId;
        this.keepAlive = builder.keepAlive;
        this.index = builder.index;
        this.searchAfter = builder.searchAfter;
        this.paginationSize = builder.paginationSize;
        this.query = builder.query;
        this.sortingOptions = builder.sortingOptions;
    }

    public static SearchPointInTimeRequest.Builder builder() {
        return new SearchPointInTimeRequest.Builder();
    }

    public static class Builder {

        private String pitId;
        private String keepAlive;
        private String index;
        private List<String> searchAfter;
        private Integer paginationSize;
        private String query;
        private List<SortingOptions> sortingOptions;


        public Builder() {

        }

        public SearchPointInTimeRequest.Builder withQuery(final String query) {
            this.query = query;
            return this;
        }

        public SearchPointInTimeRequest.Builder withSortOptions(final List<SortingOptions> sortingOptions) {
            this.sortingOptions = sortingOptions;
            return this;
        }

        public SearchPointInTimeRequest.Builder withPitId(final String pitId) {
            this.pitId = pitId;
            return this;
        }

        public SearchPointInTimeRequest.Builder withPaginationSize(final Integer paginationSize) {
            this.paginationSize = paginationSize;
            return this;
        }

        public SearchPointInTimeRequest.Builder withSearchAfter(final List<String> searchAfter) {
            this.searchAfter = searchAfter;
            return this;
        }

        public SearchPointInTimeRequest.Builder withKeepAlive(final String keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public SearchPointInTimeRequest.Builder withIndex(final String index) {
            this.index = index;
            return this;
        }

        public SearchPointInTimeRequest build() {
            return new SearchPointInTimeRequest(this);
        }
    }
}
