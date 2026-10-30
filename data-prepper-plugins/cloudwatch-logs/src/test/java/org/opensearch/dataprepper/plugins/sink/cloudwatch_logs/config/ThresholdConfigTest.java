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

        assertThat(thresholdConfig.getBatchSize(), equalTo(25));
        assertThat(thresholdConfig.getMaxEventSizeBytes(), equalTo(1048576L));
        assertThat(thresholdConfig.getMaxRequestSizeBytes(), equalTo(1048576L));
        assertThat(thresholdConfig.getFlushInterval(), equalTo(60L));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 10000})
    void GIVEN_deserialized_threshold_config_SHOULD_return_valid_batch_size(final int batchSize) {
        final Map<String, Integer> jsonMap = Map.of("batch_size", batchSize);
        final ThresholdConfig thresholdConfigTest = objectMapper.convertValue(jsonMap, ThresholdConfig.class);
        assertThat(thresholdConfigTest.getBatchSize(), equalTo(batchSize));
    }

    @ParameterizedTest
    @ValueSource(longs = {5, 300})
    void getFlushInterval_configuredDuration_returnsConfiguredSeconds(final long seconds)
            throws NoSuchFieldException, IllegalAccessException {
        final ThresholdConfig thresholdConfig = new ThresholdConfig();
        ReflectivelySetField.setField(ThresholdConfig.class, thresholdConfig, "flushInterval", Duration.ofSeconds(seconds));

        assertThat(thresholdConfig.getFlushInterval(), equalTo(seconds));
    }
}
