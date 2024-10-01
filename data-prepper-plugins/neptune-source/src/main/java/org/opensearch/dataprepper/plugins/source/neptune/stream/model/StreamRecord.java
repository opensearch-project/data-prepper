package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamRecord {

    @JsonProperty("commitTimestamp")
    private long commitTimestamp;

    @JsonProperty("eventId")
    private StreamEventId eventId;

    @JsonProperty("data")
    private StreamRecordData data;

    private String op;

    @JsonProperty("isLastOp")
    private Boolean isLastOp;

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public StreamEventId getEventId() {
        return eventId;
    }

    public StreamRecordData getData() {
        return data;
    }

    public String getOp() {
        return op;
    }
}
