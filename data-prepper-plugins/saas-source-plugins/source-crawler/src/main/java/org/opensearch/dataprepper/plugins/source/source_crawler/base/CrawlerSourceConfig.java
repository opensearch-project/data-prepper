package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import java.time.Duration;
import java.time.Instant;

/**
 * Marker interface to all the SAAS connectors configuration
 */
public interface CrawlerSourceConfig {

    int DEFAULT_NUMBER_OF_WORKERS = 1;

    /*
     * Retry settings for non-retrayble exceptions in workerPartition
     * default to 30 days to giveup retry; and 1 day to delay retry
     */
    Duration DEFAULT_MAX_DURATION_TO_GIVEUP_RETRY = Duration.ofDays(30);
    Duration DEFAULT_MAX_DURATION_TO_DELAY_RETRY = Duration.ofDays(1);

    /*
     * Delay between the current time and the end of the newest time window a crawler creates partitions for.
     * Gives the vendor API time to make late-arriving records visible before the window is queried.
     */
    Duration DEFAULT_PARTITION_CREATION_WAIT = Duration.ofMinutes(5);

    /**
     * Number of worker threads enabled for this source
     *
     * @return Integer indicating number of worker threads
     */
    int getNumberOfWorkers();

    /**
     * Boolean to indicate if acknowledgments enabled for this source
     *
     * @return boolean indicating acknowledgement state
     */
    boolean isAcknowledgments();

    /**
     * Duration to give up retrying workerPartition's work on non-retrayble exceptions
     * @return Duration indicating max duration to give up retrying 
     */
    default Duration getDurationToGiveUpRetry() {
        return DEFAULT_MAX_DURATION_TO_GIVEUP_RETRY;
    }

    /**
     * Duration to retry workerPartition's work on non-retrayble exceptions
     * @return Duration indicating max duration to delay retrying
     */
    default Duration getDurationToDelayRetry() {
        return DEFAULT_MAX_DURATION_TO_DELAY_RETRY;
    }

    /**
     * Gets the lease interval for the leader scheduler.
     * Defaults to 1 minute if not overridden.
     *
     * @return Duration for lease interval
     */
    default Duration getLeaseInterval() {
        return Duration.ofMinutes(1);
    }

    /**
     * Gets how far behind the current time partition creation lags. A time window is only
     * crawled once its end is at least this far in the past. Only used by
     * {@link DimensionalTimeSliceCrawler}, which accepts values from zero to 60 minutes.
     * Defaults to 5 minutes if not overridden.
     *
     * @return Duration to wait before creating a partition for a time window
     */
    default Duration getPartitionCreationWait() {
        return DEFAULT_PARTITION_CREATION_WAIT;
    }

    /**
     * Adjusts the start time based on the configured lookback boundary.
     * When the requested start time is before the lookback boundary and the lookback boundary
     * falls within the time window, the start time is adjusted to the lookback boundary to
     * respect API limitations (e.g., Office 365 API's maximum lookback period).
     *
     * @param startTime         the requested start time for the query
     * @param endTime           the end time for the query
     * @param lookBackStartTime the earliest time to query from (e.g., from getLookBackDuration(referenceTime))
     * @return the adjusted start time, or the original start time if no adjustment is needed
     */
    default Instant getAdjustedStartTime(final Instant startTime,
                                         final Instant endTime,
                                         final Instant lookBackStartTime) {
        if (startTime.isBefore(lookBackStartTime) && lookBackStartTime.isBefore(endTime)) {
            return lookBackStartTime;
        }
        return startTime;
    }
}
