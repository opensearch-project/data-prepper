/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.mockito.junit.jupiter.MockitoExtension;

import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_START_TIME_KEY;

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
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS.toString()));
        assertThat(countAggregateActionConfig.getMetricName(), equalTo(CountAggregateActionConfig.SUM_METRIC_NAME));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final String testCountKey = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "countKey", testCountKey);
        assertThat(countAggregateActionConfig.getCountKey(), equalTo(testCountKey));
        final String testStartTimeKey = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "startTimeKey", testStartTimeKey);
        assertThat(countAggregateActionConfig.getStartTimeKey(), equalTo(testStartTimeKey));
        final String testOutputFormat = OutputFormat.OTEL_METRICS.toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", testOutputFormat);
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS.toString()));
        final String testName = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "metricName", testName);
        assertThat(countAggregateActionConfig.getMetricName(), equalTo(testName));
    }

    @Test
    void testInvalidConfig() throws NoSuchFieldException, IllegalAccessException {
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> countAggregateActionConfig.getOutputFormat());
    }
}
