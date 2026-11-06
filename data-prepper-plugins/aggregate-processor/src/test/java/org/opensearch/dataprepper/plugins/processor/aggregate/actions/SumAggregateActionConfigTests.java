/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.SumAggregateActionConfig.DEFAULT_COUNT_KEY;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@ExtendWith(MockitoExtension.class)
public class SumAggregateActionConfigTests {
    private SumAggregateActionConfig sumAggregateActionConfig;

    private SumAggregateActionConfig createObjectUnderTest() {
        return new SumAggregateActionConfig();
    }

    @BeforeEach
    void setup() {
        sumAggregateActionConfig = createObjectUnderTest();
    }

    @Test
    void testDefault() {
        assertThat(sumAggregateActionConfig.getCountKey(), equalTo(DEFAULT_COUNT_KEY));
        assertThat(sumAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.OTEL_METRICS));
        assertThat(sumAggregateActionConfig.getMetricName(), equalTo(SumAggregateActionConfig.SUM_METRIC_NAME));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final String testKey = UUID.randomUUID().toString();
        setField(SumAggregateActionConfig.class, sumAggregateActionConfig, "key", testKey);
        assertThat(sumAggregateActionConfig.getKey(), equalTo(testKey));

        final String testCountKey = UUID.randomUUID().toString();
        setField(SumAggregateActionConfig.class, sumAggregateActionConfig, "countKey", testCountKey);
        assertThat(sumAggregateActionConfig.getCountKey(), equalTo(testCountKey));

        final OutputFormat testOutputFormat = OutputFormat.RAW;
        setField(SumAggregateActionConfig.class, sumAggregateActionConfig, "outputFormat", testOutputFormat);
        assertThat(sumAggregateActionConfig.getOutputFormat(), equalTo(OutputFormat.RAW));

        final String testName = UUID.randomUUID().toString();
        setField(SumAggregateActionConfig.class, sumAggregateActionConfig, "metricName", testName);
        assertThat(sumAggregateActionConfig.getMetricName(), equalTo(testName));
    }
}
