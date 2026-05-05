/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

public class CreateScrollResponse {

    private final String scrollId;
    private final Long scrollCreationTime;
    private final List<Event> documents;
    private final SearchShardStatistics shardStatistics;
    private final Long totalHits;

    public List<Event> getDocuments() {
        return documents;
    }

    public String getScrollId() {
        return scrollId;
    }

    public Long getScrollCreationTime() {
        return scrollCreationTime;
    }

    public SearchShardStatistics getShardStatistics() {
        return shardStatistics == null ? SearchShardStatistics.empty() : shardStatistics;
    }

    public Long getTotalHits() {
        return totalHits;
    }

    private CreateScrollResponse(final CreateScrollResponse.Builder builder) {
        this.scrollId = builder.scrollId;
        this.scrollCreationTime = builder.scrollCreationTime;
        this.documents = builder.documents;
        this.shardStatistics = builder.shardStatistics;
        this.totalHits = builder.totalHits;
    }

    public static CreateScrollResponse.Builder builder() {
        return new CreateScrollResponse.Builder();
    }

    public static class Builder {

        private List<Event> documents;
        private String scrollId;
        private Long scrollCreationTime;
        private SearchShardStatistics shardStatistics;
        private Long totalHits;

        public Builder() {

        }

        public CreateScrollResponse.Builder withDocuments(final List<Event> documents) {
            this.documents = documents;
            return this;
        }

        public CreateScrollResponse.Builder withScrollId(final String scrollId) {
            this.scrollId = scrollId;
            return this;
        }

        public CreateScrollResponse.Builder withCreationTime(final Long scrollCreationTime) {
            this.scrollCreationTime = scrollCreationTime;
            return this;
        }

        public CreateScrollResponse.Builder withShardStatistics(final SearchShardStatistics shardStatistics) {
            this.shardStatistics = shardStatistics;
            return this;
        }

        public CreateScrollResponse.Builder withTotalHits(final Long totalHits) {
            this.totalHits = totalHits;
            return this;
        }

        public CreateScrollResponse build() {
            return new CreateScrollResponse(this);
        }
    }
}
