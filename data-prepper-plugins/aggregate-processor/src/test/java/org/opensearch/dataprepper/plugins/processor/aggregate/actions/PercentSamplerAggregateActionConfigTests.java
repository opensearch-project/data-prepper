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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(MockitoExtension.class)
public class PercentSamplerAggregateActionConfigTests {
    private PercentSamplerAggregateActionConfig percentSamplerAggregateActionConfig;

    private PercentSamplerAggregateActionConfig createObjectUnderTest() {
        return new PercentSamplerAggregateActionConfig();
    }
    
    @BeforeEach
    void setup() {
        percentSamplerAggregateActionConfig = createObjectUnderTest();
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final double testPercent = ThreadLocalRandom.current().nextDouble(0.01, 99.9);
        setField(PercentSamplerAggregateActionConfig.class, percentSamplerAggregateActionConfig, "percent", testPercent);
        assertThat(percentSamplerAggregateActionConfig.getPercent(), equalTo(testPercent));
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.0, 100.0, -1.0, 110.0})
    void testInvalidConfig(double percent) throws NoSuchFieldException, IllegalAccessException {
        setField(PercentSamplerAggregateActionConfig.class, percentSamplerAggregateActionConfig, "percent", percent);
        assertThat(percentSamplerAggregateActionConfig.isPercentValid(), equalTo(false));
    }
}
