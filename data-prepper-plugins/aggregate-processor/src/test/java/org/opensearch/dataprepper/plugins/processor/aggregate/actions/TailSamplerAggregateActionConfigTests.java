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
        final int testPercent = ThreadLocalRandom.current().nextInt(1, 99);
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "percent", testPercent);
        assertThat(tailSamplerAggregateActionConfig.getPercent(), equalTo(testPercent));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 100, -1, 110})
    void testInvalidConfig(int percent) throws NoSuchFieldException, IllegalAccessException {
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
        assertThat(tailSamplerAggregateActionConfig.getCondition(), equalTo(null));
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "condition", "");
        assertTrue(tailSamplerAggregateActionConfig.getCondition().isEmpty());
    }

    @Test
    void testValidErrorCondition() throws NoSuchFieldException, IllegalAccessException {
        final String testErrorCondition = RandomStringUtils.randomAlphabetic(20);
        setField(TailSamplerAggregateActionConfig.class, tailSamplerAggregateActionConfig, "condition", testErrorCondition);
        assertThat(tailSamplerAggregateActionConfig.getCondition(), equalTo(testErrorCondition));
    }
}
