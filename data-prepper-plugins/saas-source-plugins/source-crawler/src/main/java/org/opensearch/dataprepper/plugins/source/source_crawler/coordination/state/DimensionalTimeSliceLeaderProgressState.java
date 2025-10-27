package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Instant;

@Data
public class DimensionalTimeSliceLeaderProgressState implements LeaderProgressState {

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    @JsonProperty("remaining_hours")
    private int remainingHours;

    public DimensionalTimeSliceLeaderProgressState(@JsonProperty("last_poll_time") final Instant lastPollTime,
                                          @JsonProperty("remaining_hours") int remainingHours) {
        this.lastPollTime = lastPollTime;
        this.remainingHours = remainingHours;
    }
}
