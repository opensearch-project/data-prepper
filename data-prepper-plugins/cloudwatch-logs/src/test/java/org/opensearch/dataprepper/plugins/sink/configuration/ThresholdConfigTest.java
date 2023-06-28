package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.sink.config.ThresholdConfig;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ThresholdConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void check_default_values() {
        final ThresholdConfig thresholdConfig = new ThresholdConfig();

        assertThat(thresholdConfig.getBackOffTime(), equalTo(ThresholdConfig.DEFAULT_BACKOFF_TIME));
        assertThat(thresholdConfig.getRetryCount(), equalTo(ThresholdConfig.DEFAULT_RETRY_COUNT));
        assertThat(thresholdConfig.getBatchSize(), equalTo(ThresholdConfig.DEFAULT_BATCH_SIZE));
        assertThat(thresholdConfig.getMaxEventSize(), equalTo(ThresholdConfig.DEFAULT_EVENT_SIZE));
        assertThat(thresholdConfig.getMaxRequestSize(), equalTo(ThresholdConfig.DEFAULT_SIZE_OF_REQUEST));
        assertThat(thresholdConfig.getLogSendInterval(), equalTo(ThresholdConfig.DEFAULT_LOG_SEND_INTERVAL_TIME));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void check_valid_batch_size(final int batchSize) {
        final Map<String, Integer> jsonMap = Map.of("batch_size", batchSize);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getBatchSize(), equalTo(batchSize));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 256})
    void check_valid_max_event_size(final int max_event_size) {
        final Map<String, Integer> jsonMap = Map.of("max_event_size", max_event_size);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getMaxEventSize(), equalTo(max_event_size));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 100, 1048576})
    void check_valid_request_size(final int max_batch_request_size) {
        final Map<String, Integer> jsonMap = Map.of("max_request_size", max_batch_request_size);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getMaxRequestSize(), equalTo(max_batch_request_size));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 15})
    void check_valid_retry_count(final int retry_count) {
        final Map<String, Integer> jsonMap = Map.of("retry_count", retry_count);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getRetryCount(), equalTo(retry_count));
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 10, 300})
    void check_valid_log_send_interval(final int log_send_interval) {
        final Map<String, Integer> jsonMap = Map.of("log_send_interval", log_send_interval);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getLogSendInterval(), equalTo(log_send_interval));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 100, 5000})
    void check_valid_back_off_time(final int back_off_time) {
        final Map<String, Integer> jsonMap = Map.of("back_off_time", back_off_time);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getBackOffTime(), equalTo(back_off_time));
    }
}
