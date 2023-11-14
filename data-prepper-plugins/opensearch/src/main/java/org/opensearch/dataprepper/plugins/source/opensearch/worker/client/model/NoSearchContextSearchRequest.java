/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import java.util.List;

public class NoSearchContextSearchRequest {

    private final String index;
    private final List<String> searchAfter;
    private final Integer paginationSize;

    private NoSearchContextSearchRequest(final NoSearchContextSearchRequest.Builder builder) {
        this.index = builder.index;
        this.searchAfter = builder.searchAfter;
        this.paginationSize = builder.paginationSize;
    }

    public static NoSearchContextSearchRequest.Builder builder() {
        return new NoSearchContextSearchRequest.Builder();
    }

    public String getIndex() {
        return index;
    }

    public List<String> getSearchAfter() {
        return searchAfter;
    }

    public Integer getPaginationSize() {
        return paginationSize;
    }

    public static class Builder {

        private String index;
        private List<String> searchAfter;
        private Integer paginationSize;


        public Builder() {

        }

        public NoSearchContextSearchRequest.Builder withPaginationSize(final Integer paginationSize) {
            this.paginationSize = paginationSize;
            return this;
        }

        public NoSearchContextSearchRequest.Builder withSearchAfter(final List<String> searchAfter) {
            this.searchAfter = searchAfter;
            return this;
        }

        public NoSearchContextSearchRequest.Builder withIndex(final String index) {
            this.index = index;
            return this;
        }

        public NoSearchContextSearchRequest build() {
            return new NoSearchContextSearchRequest(this);
        }
    }
}
