/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.mockito.junit.jupiter.MockitoExtension;

import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_OUTPUT_FORMAT;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.OTEL_OUTPUT_FORMAT;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
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
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(DEFAULT_OUTPUT_FORMAT));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final String testCountKey = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "countKey", testCountKey);
        assertThat(countAggregateActionConfig.getCountKey(), equalTo(testCountKey));
        final String testOutputFormat = OTEL_OUTPUT_FORMAT;
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", testOutputFormat);
        assertThat(countAggregateActionConfig.getOutputFormat(), equalTo(OTEL_OUTPUT_FORMAT));
    }

    @Test
    void testInvalidConfig() throws NoSuchFieldException, IllegalAccessException {
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> countAggregateActionConfig.getOutputFormat());
    }
}
