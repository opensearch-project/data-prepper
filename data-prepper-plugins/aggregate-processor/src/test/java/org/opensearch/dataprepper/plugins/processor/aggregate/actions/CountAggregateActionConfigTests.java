/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.mockito.junit.jupiter.MockitoExtension;

import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_START_TIME_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@ExtendWith(MockitoExtension.class)
public class CountAggregateActionConfigTests {
    private CountAggregateActionConfig countAggregateActionConfig;

    private CountAggregateActionConfig createObjectUnderTest() {
        return new CountAggregateActionConfig();
    }
    
    @BeforeEach
    void setup() {
        countAggregateActionConfig = createObjectUnderTest();
    }
    
    @Test
    void testDefault() {
        assertThat(countAggregateActionConfig.getCountKey(), equalTo(DEFAULT_COUNT_KEY));
        assertThat(countAggregateActionConfig.getStartTimeKey(), equalTo(DEFAULT_START_TIME_KEY));
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS));
        assertThat(countAggregateActionConfig.getMetricName(), equalTo(CountAggregateActionConfig.SUM_METRIC_NAME));
        assertThat(countAggregateActionConfig.getUniqueKeys(), equalTo(null));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final String testCountKey = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "countKey", testCountKey);
        assertThat(countAggregateActionConfig.getCountKey(), equalTo(testCountKey));
        final String testStartTimeKey = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "startTimeKey", testStartTimeKey);
        assertThat(countAggregateActionConfig.getStartTimeKey(), equalTo(testStartTimeKey));
        final OutputFormat testOutputFormat = OutputFormat.OTEL_METRICS;
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", testOutputFormat);
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS));
        final String testName = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "metricName", testName);
        assertThat(countAggregateActionConfig.getMetricName(), equalTo(testName));
        final List<String> uniqueKeys = new ArrayList<>();
        uniqueKeys.add(UUID.randomUUID().toString());
        uniqueKeys.add(UUID.randomUUID().toString());
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "uniqueKeys", uniqueKeys);
        assertThat(countAggregateActionConfig.getUniqueKeys(), equalTo(uniqueKeys));
    }
}
