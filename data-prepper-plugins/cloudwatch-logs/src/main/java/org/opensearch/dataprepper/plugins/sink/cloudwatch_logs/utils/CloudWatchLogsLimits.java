/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;
/**
 * CloudWatchLogsLimits receives parameters for which to reference the
 * limits of a buffer and CloudWatchLogsClient before making a
 * PutLogEvent request to AWS.
 */
public class CloudWatchLogsLimits {
    // Size of overhead for each log event message. See https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
    public static final int APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE = 26;
    private final int maxBatchSize;
    private final long maxEventSizeBytes;
    private final long maxRequestSizeBytes;
    private final long logSendInterval;

    public CloudWatchLogsLimits(final int maxBatchSize, final long maxEventSizeBytes, final long maxRequestSizeBytes, final long logSendInterval) {
        this.maxBatchSize = maxBatchSize;
        this.maxEventSizeBytes = maxEventSizeBytes;
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.logSendInterval = logSendInterval;
    }

    public boolean isTimeLimitReached(long elapsedTime) {
        return isGreaterEqualToLogSendInterval(elapsedTime);
    }

    /**
     * Checks to see if we exceed max request size
     * @param currentRequestSize size of request in bytes.
     * @param batchSize size of batch in events.
     * @return boolean true if we exceed the max request size
     */
    public boolean maxRequestSizeLimitExceeds(final long currentRequestSize, final int batchSize) {
        long bufferSizeWithOverhead = (currentRequestSize + ((long) (batchSize) * APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        return bufferSizeWithOverhead > maxRequestSizeBytes;
    }

    /**
     * Checks to see if we have reached max event count
     * @param batchSize size of batch in events.
     * @return boolean true if we exceed the max request size
     */
    public boolean isMaxEventCountLimitReached(final int batchSize) {
        return batchSize >= this.maxBatchSize;
    }

    /**
     * Checks if the interval passed in is equal to or greater
     * than the threshold interval for sending PutLogEvents.
     * @param currentTimeSeconds int denoting seconds.
     * @return boolean true if greater than or equal to logInterval, false otherwise.
     */
    private boolean isGreaterEqualToLogSendInterval(final long currentTimeSeconds) {
        return currentTimeSeconds >= logSendInterval;
    }

    /**
     * Determines if the event size is greater than the max event size.
     * @param eventSize int denoting size of event.
     * @return boolean true if greater than MaxEventSize, false otherwise.
     */
    public boolean isGreaterThanMaxEventSize(final long eventSize) {
        return (eventSize + APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE) > maxEventSizeBytes;
    }
}
