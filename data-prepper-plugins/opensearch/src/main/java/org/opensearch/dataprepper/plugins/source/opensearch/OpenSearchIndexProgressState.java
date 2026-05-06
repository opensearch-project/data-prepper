/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
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

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean hadSearchFailures;

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

    public boolean isHadSearchFailures() {
        return hadSearchFailures;
    }

    public void setHadSearchFailures(final boolean hadSearchFailures) {
        this.hadSearchFailures = hadSearchFailures;
    }

    public Map<String, Long> getFailureReasonCounts() {
        return failureReasonCounts;
    }

    public void setFailureReasonCounts(final Map<String, Long> failureReasonCounts) {
        this.failureReasonCounts = failureReasonCounts == null ? new LinkedHashMap<>() : new LinkedHashMap<>(failureReasonCounts);
    }

    /**
     * Record shard-level failures observed on a page. Sets the had-failures flag
     * and merges the per-response aggregated reason counts into the persisted
     * progress state, respecting the {@link SearchShardStatistics#MAX_DISTINCT_REASONS}
     * cap with an {@link SearchShardStatistics#OVERFLOW_REASON_KEY} overflow bucket.
     */
    public void recordShardFailures(final SearchShardStatistics shardStatistics) {
        if (shardStatistics == null || !shardStatistics.hasFailures()) {
            return;
        }
        this.hadSearchFailures = true;
        if (this.failureReasonCounts == null) {
            this.failureReasonCounts = new LinkedHashMap<>();
        }
        SearchShardStatistics.mergeFailureReasonCounts(this.failureReasonCounts, shardStatistics.getFailureReasonCounts());
    }

    /**
     * Record a per-request failure (e.g. a scroll page that threw). Normalizes
     * the exception into {@code type: message} and merges it into the aggregated
     * counts map, respecting the cap.
     */
    public void recordRequestFailure(final Throwable throwable) {
        this.hadSearchFailures = true;
        if (this.failureReasonCounts == null) {
            this.failureReasonCounts = new LinkedHashMap<>();
        }
        final String key;
        if (throwable == null) {
            key = "unknown";
        } else {
            final String message = throwable.getMessage() == null ? "" : ": " + throwable.getMessage();
            key = SearchShardStatistics.normalizeReason(throwable.getClass().getSimpleName() + message);
        }
        SearchShardStatistics.incrementFailureReasonCount(this.failureReasonCounts, key, 1L);
    }

    public boolean hasValidPointInTime() {
        return Objects.nonNull(pitId) && Objects.nonNull(pitCreationTime) && Objects.nonNull(keepAlive)
            && Instant.ofEpochMilli(pitCreationTime + keepAlive).isAfter(Instant.now());
    }
}
