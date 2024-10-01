package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_poll_time")
    private Long lastPollTime;

    public LeaderProgressState(@JsonProperty("last_poll_time") final Long lastPollTime) {
        this.lastPollTime = lastPollTime;
    }

}
