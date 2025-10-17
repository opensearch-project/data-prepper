package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Instant;

@Data
public class CrowdStrikeLeaderProgressState implements LeaderProgressState {

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    @JsonProperty("remaining_days")
    private int remainingDays;

    public CrowdStrikeLeaderProgressState(@JsonProperty("last_poll_time") final Instant lastPollTime, @JsonProperty("remaining_days") int remainingDays) {
        this.lastPollTime = lastPollTime;
        this.remainingDays = remainingDays;
    }
}
