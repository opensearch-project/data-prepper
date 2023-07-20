/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.push_condition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CloudWatchLogsLimitsTest {
    private CloudWatchLogsLimits cloudWatchLogsLimits;

    @BeforeEach
    void setUp() {
        cloudWatchLogsLimits = new CloudWatchLogsLimits(ThresholdConfig.DEFAULT_BATCH_SIZE, ThresholdConfig.DEFAULT_EVENT_SIZE * ThresholdConfig.CONVERT_TO_BYTES_FROM_KB,
                ThresholdConfig.DEFAULT_SIZE_OF_REQUEST, ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    @ParameterizedTest
    @ValueSource(ints = {257, 560, 1000})
    void check_max_event_size_invalid(final int event_size) {
        boolean isEventGreater = cloudWatchLogsLimits.isGreaterThanMaxEventSize(event_size * ThresholdConfig.CONVERT_TO_BYTES_FROM_KB);
        assertTrue(isEventGreater);
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50})
    void check_max_event_size_valid(final int event_size) {
        boolean isEventGreater = cloudWatchLogsLimits.isGreaterThanMaxEventSize((event_size * ThresholdConfig.CONVERT_TO_BYTES_FROM_KB) - CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        assertFalse(isEventGreater);
    }

    @ParameterizedTest
    @ValueSource(ints = {60, 80, 100})
    void check_greater_than_threshold_conditions_time_true(final int send_interval) {
        boolean thresholdMetTime = cloudWatchLogsLimits.isGreaterThanLimitReached(send_interval, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST,ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetTime);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 59})
    void check_greater_than_threshold_conditions_time_false(final int send_interval) {
        int validRequestSize = ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetTime = cloudWatchLogsLimits.isGreaterThanLimitReached(send_interval, validRequestSize ,ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetTime);
    }

    @ParameterizedTest
    @ValueSource(ints = {1500000, 3000000, 10000000})
    void check_greater_than_threshold_conditions_request_size_true(final int request_size) {
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, request_size, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetRequestSize);
    }

    @ParameterizedTest
    @ValueSource(ints = {10000, 250000, 500000})
    void check_greater_than_threshold_conditions_request_size_false(final int request_size) {
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME - 1, request_size, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertFalse(thresholdMetRequestSize);
    }

    @ParameterizedTest
    @ValueSource(ints = {26, 50, 100})
    void check_greater_than_threshold_conditions_batch_size_true(final int batch_size) {
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME -1, ThresholdConfig.DEFAULT_SIZE_OF_REQUEST, batch_size);
        assertTrue(thresholdMetBatchSize);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 25})
    void check_greater_than_threshold_conditions_batch_size_false(final int batch_size) {
        int validRequestSize = ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - ((ThresholdConfig.DEFAULT_BATCH_SIZE) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isGreaterThanLimitReached(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME -1, validRequestSize, batch_size);
        assertFalse(thresholdMetBatchSize);
    }

    @Test
    void check_equal_than_threshold_conditions_request_size_true() {
        int validRequestSize = ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - ((ThresholdConfig.DEFAULT_BATCH_SIZE - 1) * CloudWatchLogsLimits.APPROXIMATE_LOG_EVENT_OVERHEAD_SIZE);
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isEqualToLimitReached(validRequestSize, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertTrue(thresholdMetRequestSize);
    }

    @Test
    void check_equal_than_threshold_conditions_request_size_false() {
        boolean thresholdMetRequestSize = cloudWatchLogsLimits.isEqualToLimitReached(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - 1, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetRequestSize);
    }

    @Test
    void check_equal_than_threshold_conditions_batch_size_true() {
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isEqualToLimitReached(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - 1, ThresholdConfig.DEFAULT_BATCH_SIZE);
        assertTrue(thresholdMetBatchSize);
    }

    @Test
    void check_equal_than_threshold_conditions_batch_size_false() {
        boolean thresholdMetBatchSize = cloudWatchLogsLimits.isEqualToLimitReached(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST - 1, ThresholdConfig.DEFAULT_BATCH_SIZE - 1);
        assertFalse(thresholdMetBatchSize);
    }
}