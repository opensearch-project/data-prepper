/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.utils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.ThresholdConfig;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloudWatchLogsLimitsTest {
    private static CloudWatchLogsLimits cloudWatchLogsLimits;
    private static ThresholdConfig thresholdConfig;

    @BeforeAll
    static void setUp() {
        thresholdConfig = new ThresholdConfig();
        cloudWatchLogsLimits = new CloudWatchLogsLimits(ThresholdConfig.DEFAULT_BATCH_SIZE, thresholdConfig.getMaxEventSizeBytes(),
                thresholdConfig.getMaxRequestSizeBytes(), ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_event_size_greater_than_max_event_size_THEN_return_true() {
        boolean isEventGreater = cloudWatchLogsLimits.isGreaterThanMaxEventSize((thresholdConfig.getMaxEventSizeBytes() + 1) - CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        assertTrue(isEventGreater);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_event_size_less_than_max_event_size_THEN_return_false() {
        boolean isEventGreater = cloudWatchLogsLimits.isGreaterThanMaxEventSize(((thresholdConfig.getMaxEventSizeBytes()) - 1) - CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        assertFalse(isEventGreater);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_event_size_equal_to_max_event_size_THEN_return_false() {
        boolean isEventGreater = cloudWatchLogsLimits.isGreaterThanMaxEventSize((thresholdConfig.getMaxEventSizeBytes()) - CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        assertFalse(isEventGreater);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_log_send_interval_equal_to_max_log_send_interval_THEN_return_true() {
        boolean thresholdMetTime = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME, thresholdConfig.getMaxRequestSizeBytes(),ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetTime);
    }

    @Test
    void SGIVEN_greater_than_limit_method_WHEN_log_send_interval_greater_than_max_log_send_interval_THEN_return_true() {
        boolean thresholdMetTime = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME + 1, thresholdConfig.getMaxRequestSizeBytes(),ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetTime);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_log_send_interval_less_than_max_log_send_interval_THEN_return_false() {
        long validRequestSize = thresholdConfig.getMaxRequestSizeBytes() - ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetTime = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, validRequestSize ,ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetTime);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_request_size_greater_than_max_request_size_THEN_return_true() {
        long requestSizeWithoutOverhead = (thresholdConfig.getMaxRequestSizeBytes() + 1) - ThresholdConfig.DEFAULT_BATCH_SIZE * (CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, requestSizeWithoutOverhead, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_request_size_equal_to_max_request_size_THEN_return_false() {
        long requestSizeWithoutOverhead = (thresholdConfig.getMaxRequestSizeBytes()) - ThresholdConfig.DEFAULT_BATCH_SIZE * (CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, requestSizeWithoutOverhead, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_request_size_less_than_max_request_size_THEN_return_false() {
        long requestSizeWithoutOverhead = (thresholdConfig.getMaxRequestSizeBytes() - 1) - ThresholdConfig.DEFAULT_BATCH_SIZE * (CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, requestSizeWithoutOverhead, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_batch_size_greater_than_max_batch_size_THEN_return_true() {
        long requestSizeWithoutOverhead = (thresholdConfig.getMaxRequestSizeBytes()) - ThresholdConfig.DEFAULT_BATCH_SIZE * (CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME -1, requestSizeWithoutOverhead, ThresholdConfig.DEFAULT_BATCH_SIZE + 1);
        assertTrue(thresholdMetBatchSize);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_batch_size_equal_to_max_batch_size_THEN_return_false() {
        long validRequestSize = thresholdConfig.getMaxRequestSizeBytes() - ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME -1, validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetBatchSize);
    }

    @Test
    void GIVEN_greater_than_limit_method_WHEN_batch_size_less_than_max_batch_size_THEN_return_false() {
        long validRequestSize = thresholdConfig.getMaxRequestSizeBytes()- ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME -1, validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetBatchSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_request_size_equal_to_max_batch_size_THEN_return_true() {
        long validRequestSize = thresholdConfig.getMaxRequestSizeBytes() - ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_request_size_greater_than_max_batch_size_THEN_return_false() {
        long validRequestSize = ((thresholdConfig.getMaxRequestSizeBytes() + 1) - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_request_size_less_than_max_batch_size_THEN_return_false() {
        long validRequestSize = ((thresholdConfig.getMaxRequestSizeBytes() - 1) - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetRequestSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_batch_size_equal_to_max_batch_size_THEN_return_true() {
        long validRequestSize = ((thresholdConfig.getMaxRequestSizeBytes() - 1) - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isEqualToLimitReached(thresholdConfig.getMaxRequestSizeBytes(), ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetBatchSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_batch_size_greater_than_max_batch_size_THEN_return_false() {
        long validRequestSize = ((thresholdConfig.getMaxRequestSizeBytes() - 1) - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE + 1);
        assertFalse(thresholdMetBatchSize);
    }

    @Test
    void GIVEN_equal_to_limit_method_WHEN_batch_size_less_than_max_batch_size_THEN_return_false() {
        long validRequestSize = ((thresholdConfig.getMaxRequestSizeBytes() - 1) - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE));
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetBatchSize);
    }
}