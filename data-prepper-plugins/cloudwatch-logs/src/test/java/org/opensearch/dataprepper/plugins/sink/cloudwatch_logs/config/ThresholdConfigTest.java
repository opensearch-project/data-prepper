/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ThresholdConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void GIVEN_new_threshold_config_SHOULD_return_valid_default_values() {
        final ThresholdConfig thresholdConfig = new ThresholdConfig();

        assertThat(thresholdConfig.getBackOffTime(), equalTo(ThresholdConfig.DEFAULT_BACKOFF_TIME));
        assertThat(thresholdConfig.getRetryCount(), equalTo(ThresholdConfig.DEFAULT_RETRY_COUNT));
        assertThat(thresholdConfig.getBatchSize(), equalTo(ThresholdConfig.DEFAULT_BATCH_SIZE));
        assertThat(thresholdConfig.getMaxEventSizeBytes(), equalTo(ByteCount.parse(ThresholdConfig.DEFAULT_EVENT_SIZE).getBytes()));
        assertThat(thresholdConfig.getMaxRequestSizeBytes(), equalTo(ByteCount.parse(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST).getBytes()));
        assertThat(thresholdConfig.getLogSendInterval(), equalTo(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_batch_size(final int batchSize) {
        final Map<String, Integer> jsonMap = Map.of("batch_size", batchSize);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getBatchSize(), equalTo(batchSize));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1kb", "10kb", "256kb"})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_max_event_size(final String max_event_size) throws NoSuchFieldException, IllegalAccessException {
        ThresholdConfig sampleThresholdConfig = new ThresholdConfig();
        ReflectivelySetField.setField(sampleThresholdConfig.getClass(), sampleThresholdConfig, "maxEventSize", max_event_size);
        assertThat(sampleThresholdConfig.getMaxEventSizeBytes(), equalTo(ByteCount.parse(max_event_size).getBytes()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1b", "100b", "1048576b"})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_max_request_size(final String max_batch_request_size) throws NoSuchFieldException, IllegalAccessException {
        ThresholdConfig sampleThresholdConfig = new ThresholdConfig();
        ReflectivelySetField.setField(sampleThresholdConfig.getClass(), sampleThresholdConfig, "maxRequestSize", max_batch_request_size);
        assertThat(sampleThresholdConfig.getMaxRequestSizeBytes(), equalTo(ByteCount.parse(max_batch_request_size).getBytes()));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 15})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_max_retry_count(final int retry_count) {
        final Map<String, Integer> jsonMap = Map.of("retry_count", retry_count);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getRetryCount(), equalTo(retry_count));
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 10, 300})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_max_log_send_interval(final int log_send_interval) throws NoSuchFieldException, IllegalAccessException {
        ThresholdConfig sampleThresholdConfig = new ThresholdConfig();
        ReflectivelySetField.setField(sampleThresholdConfig.getClass(), sampleThresholdConfig, "logSendInterval", Duration.ofSeconds(log_send_interval));
        assertThat(sampleThresholdConfig.getLogSendInterval(), equalTo(Duration.ofSeconds(log_send_interval).getSeconds())) ;
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 500, 1000})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_back_off_time(final long back_off_time) throws NoSuchFieldException, IllegalAccessException {
        ThresholdConfig sampleThresholdConfig = new ThresholdConfig();
        ReflectivelySetField.setField(sampleThresholdConfig.getClass(), sampleThresholdConfig, "backOffTime", Duration.ofMillis(back_off_time));
        assertThat(sampleThresholdConfig.getBackOffTime(), equalTo(back_off_time));
    }
}