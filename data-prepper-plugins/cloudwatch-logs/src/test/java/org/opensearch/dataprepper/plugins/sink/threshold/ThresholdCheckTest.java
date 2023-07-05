/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.threshold;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ThresholdCheckTest {
    private ThresholdCheck thresholdCheck;

    @BeforeEach
    void setUp() {
        thresholdCheck = new ThresholdCheck(ThresholdConfig.DEFAULT_BATCH_SIZE, ThresholdConfig.DEFAULT_EVENT_SIZE,
                ThresholdConfig.DEFAULT_SIZE_OF_REQUEST, ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void check_batchSize_invalid(final int batch_size) {
        assertThat(thresholdCheck.checkBatchSize(batch_size), is(false));
    }

    @Test
    void check_batchSize_valid() {
        assertThat(thresholdCheck.checkBatchSize(ThresholdConfig.DEFAULT_BATCH_SIZE), is(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50})
    void check_log_send_interval_invalid(final int send_interval) {
        assertThat(thresholdCheck.checkLogSendInterval(send_interval), is(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {60, 80, 100})
    void check_log_send_interval_valid(final int send_interval) {
        assertThat(thresholdCheck.checkLogSendInterval(send_interval), is(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {55, 80, 100})
    void check_max_event_size_invalid(final int event_size) {
        assertThat(thresholdCheck.checkMaxEventSize(event_size), is(true));
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 30, 50})
    void check_max_event_size_valid(final int event_size) {
        assertThat(thresholdCheck.checkMaxEventSize(event_size), is(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {10000, 250000, 500000})
    void check_max_request_size_invalid(final int request_size) {
        assertThat(thresholdCheck.checkMaxRequestSize(request_size), is(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {550000, 750000, 1000000})
    void check_max_request_size_valid(final int request_size) {
        assertThat(thresholdCheck.checkMaxRequestSize(request_size), is(true));
    }
}
