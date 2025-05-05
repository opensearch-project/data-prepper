package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Instant;
@Data
public class AtlassianLeaderProgressState implements LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    public AtlassianLeaderProgressState(@JsonProperty("last_poll_time") final Instant lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
}

