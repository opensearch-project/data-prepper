/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class OpenSearchIndexProgressState {

    @JsonInclude(JsonInclude.Include. NON_NULL)
    private String pitId;

    @JsonInclude(JsonInclude.Include. NON_NULL)
    private Long pitCreationTime;

    @JsonInclude(JsonInclude.Include. NON_NULL)
    private Long keepAlive;

    @JsonInclude(JsonInclude.Include. NON_NULL)
    private List<String> searchAfter;

    public OpenSearchIndexProgressState() {

    }

    @JsonCreator
    public OpenSearchIndexProgressState(@JsonProperty("pit_id") final String pitId,
                                        @JsonProperty("pit_creation_time") final Long pitCreationTime,
                                        @JsonProperty("pit_keep_alive") final Long pitKeepAlive,
                                        @JsonProperty("pit_search_after") final List<String> searchAfter) {
        this.pitId = pitId;
        this.pitCreationTime = pitCreationTime;
        this.keepAlive = pitKeepAlive;
        this.searchAfter = searchAfter;
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

    public boolean hasValidPointInTime() {
        return Objects.nonNull(pitId) && Objects.nonNull(pitCreationTime) && Objects.nonNull(keepAlive)
            && Instant.ofEpochMilli(pitCreationTime + keepAlive).isAfter(Instant.now());
    }
}
