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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
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
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecondByGroup(), empty());
    }

    @Test
    void testValidConfig() throws NoSuchFieldException, IllegalAccessException {
        final int testEventsPerSecond = ThreadLocalRandom.current().nextInt();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "whenExceedsMode",  RateLimiterMode.fromOptionValue("drop"));

        final RateLimiterAggregateActionConfig.GroupRateLimit groupRateLimit = new RateLimiterAggregateActionConfig.GroupRateLimit();
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, groupRateLimit, "key", Map.of("/organization", "A", "/region", "us-east-1"));
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, groupRateLimit, "eventsPerSecond", 5000);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecondByGroup", List.of(groupRateLimit));

        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecond(), equalTo(testEventsPerSecond));
        assertThat(rateLimiterAggregateActionConfig.getWhenExceeds(), equalTo(RateLimiterMode.DROP));
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecondByGroup(), hasSize(1));
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecondByGroup().get(0).getKey(),
                equalTo(Map.of("/organization", "A", "/region", "us-east-1")));
        assertThat(rateLimiterAggregateActionConfig.getEventsPerSecondByGroup().get(0).getEventsPerSecond(), equalTo(5000));
    }

    @Test
    void isEventsPerSecondByGroupKeysUnique_returns_false_when_duplicate_keys() throws NoSuchFieldException, IllegalAccessException {
        final int testEventsPerSecond = ThreadLocalRandom.current().nextInt();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "whenExceedsMode",  RateLimiterMode.fromOptionValue("drop"));

        final RateLimiterAggregateActionConfig.GroupRateLimit groupA = new RateLimiterAggregateActionConfig.GroupRateLimit();
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, groupA, "key", Map.of("/organization", "A", "/region", "us-east-1"));
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, groupA, "eventsPerSecond", 5000);

        final RateLimiterAggregateActionConfig.GroupRateLimit duplicate = new RateLimiterAggregateActionConfig.GroupRateLimit();
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, duplicate, "key", Map.of("/organization", "A", "/region", "us-east-1"));
        setField(RateLimiterAggregateActionConfig.GroupRateLimit.class, duplicate, "eventsPerSecond", 2000);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecondByGroup", List.of(groupA, duplicate));

        assertThat(rateLimiterAggregateActionConfig.isEventsPerSecondByGroupKeysUnique(), equalTo(false));
    }
}