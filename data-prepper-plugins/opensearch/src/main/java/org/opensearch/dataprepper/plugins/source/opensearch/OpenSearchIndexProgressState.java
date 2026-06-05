/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchShardStatistics;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OpenSearchIndexProgressState {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String pitId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long pitCreationTime;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Long keepAlive;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> searchAfter;

    @JsonProperty("had_search_failures")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean hadSearchFailures;

    @JsonProperty("failure_reason_counts")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, Long> failureReasonCounts = new LinkedHashMap<>();

    public OpenSearchIndexProgressState() {

    }

    @JsonCreator
    public OpenSearchIndexProgressState(@JsonProperty("pit_id") final String pitId,
                                        @JsonProperty("pit_creation_time") final Long pitCreationTime,
                                        @JsonProperty("pit_keep_alive") final Long pitKeepAlive,
                                        @JsonProperty("pit_search_after") final List<String> searchAfter,
                                        @JsonProperty("had_search_failures") final boolean hadSearchFailures,
                                        @JsonProperty("failure_reason_counts") final Map<String, Long> failureReasonCounts) {
        this.pitId = pitId;
        this.pitCreationTime = pitCreationTime;
        this.keepAlive = pitKeepAlive;
        this.searchAfter = searchAfter;
        this.hadSearchFailures = hadSearchFailures;
        this.failureReasonCounts = failureReasonCounts == null ? new LinkedHashMap<>() : new LinkedHashMap<>(failureReasonCounts);
    }

    public List<String> getSearchAfter() {
        return searchAfter;
    }

    public void setSearchAfter(List<String> searchAfter) {
        this.searchAfter = searchAfter;
    }

    public String getPitId() {
        return pitId;
    }

    public void setPitId(final String pitId) {
        this.pitId = pitId;
    }

    public Long getPitCreationTime() {
        return pitCreationTime;
    }

    public void setPitCreationTime(final Long pitCreationTime) {
        this.pitCreationTime = pitCreationTime;
    }

    public Long getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(final Long keepAlive) {
        this.keepAlive = keepAlive;
    }

    @JsonIgnore
    private transient ShardFailureAggregator failureAggregator;

    private ShardFailureAggregator getOrCreateAggregator() {
        if (failureAggregator == null) {
            failureAggregator = new ShardFailureAggregator(hadSearchFailures, failureReasonCounts);
        }
        return failureAggregator;
    }

    private void syncFromAggregator() {
        hadSearchFailures = failureAggregator.hadFailures();
        failureReasonCounts = new LinkedHashMap<>(failureAggregator.getFailureReasonCounts());
    }

    public boolean isHadSearchFailures() {
        return getOrCreateAggregator().hadFailures();
    }

    public void setHadSearchFailures(final boolean hadSearchFailures) {
        this.hadSearchFailures = hadSearchFailures;
        this.failureAggregator = null;
    }

    public Map<String, Long> getFailureReasonCounts() {
        return getOrCreateAggregator().getFailureReasonCounts();
    }

    public void setFailureReasonCounts(final Map<String, Long> failureReasonCounts) {
        this.failureReasonCounts = failureReasonCounts == null ? new LinkedHashMap<>() : new LinkedHashMap<>(failureReasonCounts);
        this.failureAggregator = null;
    }

    public void recordShardFailures(final SearchShardStatistics shardStatistics) {
        getOrCreateAggregator().recordShardFailures(shardStatistics);
        syncFromAggregator();
    }

    public void recordRequestFailure(final Throwable throwable) {
        getOrCreateAggregator().recordRequestFailure(throwable);
        syncFromAggregator();
    }

    public boolean hasValidPointInTime() {
        return Objects.nonNull(pitId) && Objects.nonNull(pitCreationTime) && Objects.nonNull(keepAlive)
            && Instant.ofEpochMilli(pitCreationTime + keepAlive).isAfter(Instant.now());
    }
}
