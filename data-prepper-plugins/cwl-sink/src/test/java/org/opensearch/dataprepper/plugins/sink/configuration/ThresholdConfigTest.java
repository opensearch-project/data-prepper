package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
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
        assertThat(thresholdConfigTest.getMaxBatchSize(), equalTo(max_batch_request_size));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 15})
    void check_valid_retry_count(final int retryCount) {
        final Map<String, Integer> jsonMap = Map.of("retry_count", retryCount);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getRetryCount(), equalTo(retryCount));
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 10, 300})
    void check_valid_log_send_interval(final int logSendInterval) {
        final Map<String, Integer> jsonMap = Map.of("log_send_interval", logSendInterval);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getLogSendInterval(), equalTo(logSendInterval));
    }
}
