/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RateLimiterAggregateActionTests {
    AggregateActionInput aggregateActionInput;

    private AggregateAction rateLimiterAggregateAction;
    @Mock
    private RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig;

    private AggregateAction createObjectUnderTest(RateLimiterAggregateActionConfig config) {
        return new RateLimiterAggregateAction(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 100, 1000})
    void testRateLimiterAggregateSmoothTraffic(int testEventsPerSecond) throws InterruptedException {
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(testEventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.DROP.toString());
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
        final AggregateActionOutput actionOutput = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 200, 500, 1000})
    void testRateLimiterInDropMode(int testEventsPerSecond) throws InterruptedException {
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(testEventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.DROP.toString());
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
        final AggregateActionOutput actionOutput = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertTrue(result.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 200, 500, 1000})
    void testRateLimiterInBlockMode(int testEventsPerSecond) throws InterruptedException {
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(testEventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.BLOCK.toString());
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
        final AggregateActionOutput actionOutput = rateLimiterAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertTrue(result.isEmpty());
    }
}
