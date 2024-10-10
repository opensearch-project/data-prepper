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
        assertThat(rateLimiterAggregateActionConfig.getWhenExceeds(), equalTo(RateLimiterMode.BLOCK));
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final int testEventsPerSecond = ThreadLocalRandom.current().nextInt();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "whenExceedsMode",  RateLimiterMode.fromOptionValue("drop"));
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecond(), equalTo(testEventsPerSecond));
        assertThat(rateLimiterAggregateActionConfig.getWhenExceeds(), equalTo(RateLimiterMode.DROP));
    }
}
