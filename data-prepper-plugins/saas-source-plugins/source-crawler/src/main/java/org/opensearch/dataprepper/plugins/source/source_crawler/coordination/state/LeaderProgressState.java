package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.source_crawler.util.CustomInstantDeserializer;

import java.time.Instant;

@Setter
@Getter
public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_poll_time")
    @JsonDeserialize(using = CustomInstantDeserializer.class)
    private Instant lastPollTime;

    public LeaderProgressState(@JsonProperty("last_poll_time") final Instant lastPollTime) {
        this.lastPollTime = lastPollTime;
    }
}

