package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Setter
@Getter
public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    public LeaderProgressState(@JsonProperty("last_poll_time") final Instant lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
}

