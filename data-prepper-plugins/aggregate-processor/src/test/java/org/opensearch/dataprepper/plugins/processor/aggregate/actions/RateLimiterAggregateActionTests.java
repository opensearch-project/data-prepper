/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class RateLimiterAggregateActionTests {
    AggregateActionInput aggregateActionInput;

    private AggregateAction rateLimiterAggregateAction;

    private AggregateAction createObjectUnderTest(RateLimiterAggregateActionConfig config) {
        return new RateLimiterAggregateAction(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 100, 1000})
    void testRateLimiterAggregateSmoothTraffic(int testEventsPerSecond) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = new RateLimiterAggregateActionConfig();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        rateLimiterAggregateAction = createObjectUnderTest(rateLimiterAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        for (int i = 0; i < testEventsPerSecond; i++) { 
            final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
            testEvent.put(dataKey, UUID.randomUUID().toString());
            final AggregateActionResponse aggregateActionResponse = rateLimiterAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(testEvent));
            Thread.sleep(1000/testEventsPerSecond);
        }
        final Optional<Event> result = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 200, 500, 1000})
    void testRateLimiterAggregateFailuresBurstTraffic(int testEventsPerSecond) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = new RateLimiterAggregateActionConfig();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        rateLimiterAggregateAction = createObjectUnderTest(rateLimiterAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        int numFailed = 0;
        for (int i = 0; i < testEventsPerSecond; i++) { 
            final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
            testEvent.put(dataKey, UUID.randomUUID().toString());
            final AggregateActionResponse aggregateActionResponse = rateLimiterAggregateAction.handleEvent(testEvent, aggregateActionInput);
            if (aggregateActionResponse.getEvent() == null) {
                numFailed++;
            } 
        }
        assertThat(numFailed, greaterThan(0));
        final Optional<Event> result = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result.isPresent(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 200, 500, 1000})
    void testRateLimiterAggregateSuccessWithBurstTraffic(int testEventsPerSecond) throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = new RateLimiterAggregateActionConfig();
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "eventsPerSecond", testEventsPerSecond);
        setField(RateLimiterAggregateActionConfig.class, rateLimiterAggregateActionConfig, "dropWhenExceeds", false);
        rateLimiterAggregateAction = createObjectUnderTest(rateLimiterAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        int numFailed = 0;
        for (int i = 0; i < testEventsPerSecond; i++) { 
            final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
            testEvent.put(dataKey, UUID.randomUUID().toString());
            final AggregateActionResponse aggregateActionResponse = rateLimiterAggregateAction.handleEvent(testEvent, aggregateActionInput);
            if (aggregateActionResponse.getEvent() == null) {
                numFailed++;
            }  else {
                assertThat(aggregateActionResponse.getEvent(), equalTo(testEvent));
            }
        }
        assertThat(numFailed, equalTo(0));
        final Optional<Event> result = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result.isPresent(), equalTo(false));
    }
}
