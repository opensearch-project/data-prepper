package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("streamArns")
    private List<String> streamArns;

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    public List<String> getStreamArns() {
        return streamArns;
    }

    public void setStreamArns(List<String> streamArns) {
        this.streamArns = streamArns;
    }
}
