/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.lang3.RandomStringUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
public class RateLimiterAggregateActionConfigTests {
    private RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig;

    private RateLimiterAggregateActionConfig createObjectUnderTest() {
        return new RateLimiterAggregateActionConfig();
    }
    
    @BeforeEach
    void setup() {
        rateLimiterAggregateActionConfig = createObjectUnderTest();
    }

    @Test
    void testDefault() {
        assertThat(rateLimiterAggregateActionConfig.getWhenExceeds(), equalTo(RateLimiterMode.BLOCK.toString()));
    }

    @Test
    void testInvalidConfig() throws NoSuchFieldException, IllegalAccessException {
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "whenExceedsMode", RandomStringUtils.randomAlphabetic(4));
         assertThrows(IllegalArgumentException.class, () -> rateLimiterAggregateActionConfig.getWhenExceeds());
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final int testEventsPerSecond = ThreadLocalRandom.current().nextInt();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "whenExceedsMode", "drop");
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecond(), equalTo(testEventsPerSecond));
        assertThat(rateLimiterAggregateActionConfig.getWhenExceeds(), equalTo(RateLimiterMode.DROP.toString()));
    }
}
