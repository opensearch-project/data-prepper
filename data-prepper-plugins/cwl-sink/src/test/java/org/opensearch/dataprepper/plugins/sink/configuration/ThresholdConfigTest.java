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
}
