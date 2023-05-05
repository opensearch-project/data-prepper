/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.commons.lang3.RandomStringUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

@ExtendWith(MockitoExtension.class)
public class TailSamplerAggregateActionConfigTests {
    private TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig;

    private TailSamplerAggregateActionConfig createObjectUnderTest() {
        return new TailSamplerAggregateActionConfig();
    }
    
    @BeforeEach
    void setup() {
        tailSamplerAggregateActionConfig = createObjectUnderTest();
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final double testPercent = ThreadLocalRandom.current().nextDouble(0.01, 99.9);
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "percent", testPercent);
        assertThat(tailSamplerAggregateActionConfig.getPercent(), equalTo(testPercent));
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.0, 100.0, -1.0, 110.0})
    void testInvalidConfig(double percent) throws NoSuchFieldException, IllegalAccessException {
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "percent", percent);
        assertThat(tailSamplerAggregateActionConfig.isPercentValid(), equalTo(false));
    }

    @Test
    void testWaitPeriod() throws NoSuchFieldException, IllegalAccessException {
        final Duration testWaitPeriod = Duration.ofSeconds(ThreadLocalRandom.current().nextInt(1, 600));
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "waitPeriod", testWaitPeriod);
        assertThat(tailSamplerAggregateActionConfig.getWaitPeriod(), equalTo(testWaitPeriod));
    }

    @Test
    void testErrorConditionEmptyOrNull() throws NoSuchFieldException, IllegalAccessException {
        assertThat(tailSamplerAggregateActionConfig.getErrorCondition(), equalTo(null));
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "errorCondition", "");
        assertTrue(tailSamplerAggregateActionConfig.getErrorCondition().isEmpty());
    }

    @Test
    void testValidErrorCondition() throws NoSuchFieldException, IllegalAccessException {
        final String testErrorCondition = RandomStringUtils.randomAlphabetic(20);
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "errorCondition", testErrorCondition);
        assertThat(tailSamplerAggregateActionConfig.getErrorCondition(), equalTo(testErrorCondition));
    }
}
