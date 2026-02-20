package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Duration;
import java.time.Instant;

/**
 * Leader progress state for dimensional time slice crawler.
 * Uses Instant-based remainingDuration for precise time-based historical pulls.
 *
 * <p>Backward Compatibility: This class supports deserialization of legacy formats:
 * <ul>
 *   <li>Preferred format: {@code remaining_duration} (Instant)</li>
 *   <li>Legacy format: {@code remaining_hours} (int) - automatically converted to remainingDuration</li>
 * </ul>
 */
@Data
public class DimensionalTimeSliceLeaderProgressState implements LeaderProgressState {

    private static final long MINUTES_PER_HOUR = 60;

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    @JsonProperty("remaining_duration")
    private Instant remainingDuration;


    /**
     * Primary constructor supporting both new and legacy formats.
     *
     * @param lastPollTime the last poll timestamp
     * @param remainingDuration the remaining duration as Instant (preferred format)
     */
    @JsonCreator
    public DimensionalTimeSliceLeaderProgressState(
            @JsonProperty("last_poll_time") final Instant lastPollTime,
            @JsonProperty("remaining_duration") Instant remainingDuration) {
        this.lastPollTime = lastPollTime;

        // Prefer remaining_duration (Instant) if provided
        if (remainingDuration != null && remainingDuration.isBefore(lastPollTime)) {
            this.remainingDuration = remainingDuration;
        } else {
            this.remainingDuration = lastPollTime;
        }
    }

    /**
     * Backward-compatible constructor for existing connectors that pass hours as int.
     * This constructor is specifically for external connectors that use getLookBackHours()
     * which returns an int representing hours.
     *
     * @param lastPollTime the last poll timestamp
     * @param remainingHours the remaining hours for historical pull (will be converted to remainingDuration)
     */
    public DimensionalTimeSliceLeaderProgressState(final Instant lastPollTime, int remainingHours) {
        this.lastPollTime = lastPollTime;
        this.remainingDuration = remainingDurationFromHours(lastPollTime, remainingHours);
    }

    /**
     * Backward compatibility setter for legacy remaining_hours field.
     * Converts hours to remainingDuration when deserializing old checkpoint data.
     *
     * @param remainingHours the remaining hours (will be converted to remainingDuration)
     */
    @JsonSetter("remaining_hours")
    public void setRemainingHours(int remainingHours) {
        if (this.remainingDuration == null || this.remainingDuration.equals(this.lastPollTime)) {
            this.remainingDuration = remainingDurationFromHours(this.lastPollTime, remainingHours);
        }
    }

    private Instant remainingDurationFromHours(Instant lastPollTime, int remainingHours) {
        if (lastPollTime == null) {
            return null;
        }
        long minutes = remainingHours * MINUTES_PER_HOUR;
        if (minutes > 0) {
            return lastPollTime.minus(Duration.ofMinutes(minutes));
        }
        return lastPollTime;
    }

    /**
     * Provides backward compatible getter for remaining hours.
     *
     * @return the remaining time in hours (rounded down from minutes)
     * @deprecated Use {@link #getRemainingDuration()} for Instant-based granularity
     */
    @Deprecated
    @JsonIgnore
    public int getRemainingHours() {
        if (remainingDuration != null && lastPollTime != null && remainingDuration.isBefore(lastPollTime)) {
            long minutes = Duration.between(remainingDuration, lastPollTime).toMinutes();
            return (int) (minutes / MINUTES_PER_HOUR);
        }
        return 0;
    }
}
