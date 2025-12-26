package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Instant;

/**
 * Leader progress state for dimensional time slice crawler.
 * Supports minute-level granularity for historical pulls.
 *
 * <p>Backward Compatibility: This class supports deserialization of both:
 * <ul>
 *   <li>New format: {@code remaining_minutes} (long) - preferred</li>
 *   <li>Old format: {@code remaining_hours} (int) - automatically converted to minutes</li>
 * </ul>
 */
@Data
public class DimensionalTimeSliceLeaderProgressState implements LeaderProgressState {

    private static final long MINUTES_PER_HOUR = 60;

    @JsonProperty("last_poll_time")
    private Instant lastPollTime;

    @JsonProperty("remaining_minutes")
    private long remainingMinutes;


    /**
     * Primary constructor supporting both new and legacy formats.
     *
     * @param lastPollTime the last poll timestamp
     * @param remainingMinutes the remaining minutes for historical pull (new format)
     * @param remainingHours the remaining hours for historical pull (legacy format, converted to minutes)
     */
    @JsonCreator
    public DimensionalTimeSliceLeaderProgressState(
            @JsonProperty("last_poll_time") final Instant lastPollTime,
            @JsonProperty("remaining_minutes") Long remainingMinutes,
            @JsonProperty("remaining_hours") Integer remainingHours) {
        this.lastPollTime = lastPollTime;

        // Prefer remaining_minutes if provided, otherwise convert from remaining_hours
        if (remainingMinutes != null) {
            this.remainingMinutes = remainingMinutes;
        } else if (remainingHours != null) {
            // Backward compatibility: convert hours to minutes
            this.remainingMinutes = remainingHours * MINUTES_PER_HOUR;
        } else {
            this.remainingMinutes = 0;
        }
    }

    /**
     * Convenience constructor for creating new state with minutes.
     *
     * @param lastPollTime the last poll timestamp
     * @param remainingMinutes the remaining minutes for historical pull
     */
    public DimensionalTimeSliceLeaderProgressState(final Instant lastPollTime, long remainingMinutes) {
        this(lastPollTime, remainingMinutes, null);
    }

    /**
     * Backward-compatible constructor for existing connectors that pass hours as int.
     * This constructor is specifically for external connectors that use getLookBackHours()
     * which returns an int representing hours.
     *
     * <p>Note: Java would auto-promote int to long for the minutes constructor,
     * so we need this explicit int constructor to maintain backward compatibility
     * with connectors passing hours.
     *
     * @param lastPollTime the last poll timestamp
     * @param remainingHours the remaining hours for historical pull (will be converted to minutes)
     * @deprecated Use the long constructor with minutes directly for new implementations
     */
    @Deprecated
    public DimensionalTimeSliceLeaderProgressState(final Instant lastPollTime, int remainingHours) {
        this(lastPollTime, null, remainingHours);
    }

    /**
     * Backward compatibility setter for legacy remaining_hours field.
     * Converts hours to minutes when deserializing old checkpoint data.
     *
     * @param remainingHours the remaining hours (will be converted to minutes)
     */
    @JsonSetter("remaining_hours")
    public void setRemainingHours(int remainingHours) {
        // Only set if remainingMinutes hasn't been set yet (prefer minutes to hours)
        if (this.remainingMinutes == 0) {
            this.remainingMinutes = remainingHours * MINUTES_PER_HOUR;
        }
    }

    /**
     * Provides backward compatible getter for remaining hours.
     *
     * @return the remaining time in hours (rounded down from minutes)
     * @deprecated Use {@link #getRemainingMinutes()} for minute-level granularity
     */
    @Deprecated
    @JsonIgnore
    public int getRemainingHours() {
        return (int) (remainingMinutes / MINUTES_PER_HOUR);
    }
}
