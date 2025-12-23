package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import java.time.Duration;

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
}
