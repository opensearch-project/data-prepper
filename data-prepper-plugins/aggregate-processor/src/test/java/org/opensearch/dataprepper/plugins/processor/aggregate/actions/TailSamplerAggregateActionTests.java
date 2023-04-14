/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.CoreMatchers.not;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
public class TailSamplerAggregateActionTests {
    AggregateActionInput aggregateActionInput;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private AggregateAction tailSamplerAggregateAction;

    @Mock
    private TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig;

    private AggregateAction createObjectUnderTest(TailSamplerAggregateActionConfig config) {
        return new TailSamplerAggregateAction(config, expressionEvaluator);
    }

    @Test
    void testTailSamplerAggregateBasic() throws InterruptedException {
        final Duration testWaitPeriod = Duration.ofSeconds(3);
        final double testPercent = 100;
        when(tailSamplerAggregateActionConfig.getPercent()).thenReturn(testPercent);
        when(tailSamplerAggregateActionConfig.getWaitPeriod()).thenReturn(testWaitPeriod);
        when(tailSamplerAggregateActionConfig.getErrorCondition()).thenReturn("");
        tailSamplerAggregateAction = createObjectUnderTest(tailSamplerAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final int totalEvents = 100;
        int allowedEvents = 0;
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        for (int i = 0; i < totalEvents; i++) { 
            final String dataValue = UUID.randomUUID().toString();
            testEvent.put(dataKey, dataValue);
            final AggregateActionResponse aggregateActionResponse = tailSamplerAggregateAction.handleEvent(testEvent, aggregateActionInput);
            Event receivedEvent = aggregateActionResponse.getEvent();
            assertThat(receivedEvent, equalTo(null));
        }
        List<Event> result = tailSamplerAggregateAction.concludeGroup(aggregateActionInput);
        assertTrue(result.isEmpty());
        Thread.sleep(2 * testWaitPeriod.toMillis());
        result = tailSamplerAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result, not(equalTo(null)));
        assertThat(result.size(), equalTo(totalEvents));
    }

    @Test
    void testTailSamplerAggregateWithErrorCondition() throws InterruptedException {
        final Duration testWaitPeriod = Duration.ofSeconds(3);
        final double testPercent = 0;
        final String statusKey = "status";
        final int errorStatusValue = 1;
        final String errorCondition = "/"+statusKey+" == "+errorStatusValue;
        when(tailSamplerAggregateActionConfig.getPercent()).thenReturn(testPercent);
        when(tailSamplerAggregateActionConfig.getWaitPeriod()).thenReturn(testWaitPeriod);
        when(tailSamplerAggregateActionConfig.getErrorCondition()).thenReturn(errorCondition);
        when(expressionEvaluator.evaluate(any(String.class), any(Event.class))).thenReturn(true);
        tailSamplerAggregateAction = createObjectUnderTest(tailSamplerAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final int totalEvents = 100;
        int allowedEvents = 0;
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        for (int i = 0; i < totalEvents; i++) { 
            final String dataValue = UUID.randomUUID().toString();
            testEvent.put(statusKey, errorStatusValue);
            testEvent.put(dataKey, dataValue);
            final AggregateActionResponse aggregateActionResponse = tailSamplerAggregateAction.handleEvent(testEvent, aggregateActionInput);
            Event receivedEvent = aggregateActionResponse.getEvent();
            assertThat(receivedEvent, equalTo(null));
        }
        List<Event> result = tailSamplerAggregateAction.concludeGroup(aggregateActionInput);
        assertTrue(result.isEmpty());
        Thread.sleep(2 * testWaitPeriod.toMillis());
        result = tailSamplerAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result, not(equalTo(null)));
        assertThat(result.size(), equalTo(totalEvents));
    }
}
