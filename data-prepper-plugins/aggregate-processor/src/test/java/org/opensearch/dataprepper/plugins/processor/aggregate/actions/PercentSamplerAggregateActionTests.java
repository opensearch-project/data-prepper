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

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class PercentSamplerAggregateActionTests {
    AggregateActionInput aggregateActionInput;

    private AggregateAction percentSamplerAggregateAction;

    @Mock
    private PercentSamplerAggregateActionConfig percentSamplerAggregateActionConfig;

    private AggregateAction createObjectUnderTest(PercentSamplerAggregateActionConfig config) {
        return new PercentSamplerAggregateAction(config);
    }

    @ParameterizedTest
    @ValueSource(doubles = {1.0, 10.0, 25.0, 50.0, 66.0, 75.0, 90.0, 99.0})
    void testPercentSamplerAggregate(double testPercent) throws InterruptedException {
        when(percentSamplerAggregateActionConfig.getPercent()).thenReturn(testPercent);
        percentSamplerAggregateAction = createObjectUnderTest(percentSamplerAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final int totalEvents = 1000;
        int allowedEvents = 0;
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        for (int i = 0; i < totalEvents; i++) { 
            final String dataValue = UUID.randomUUID().toString();
            testEvent.put(dataKey, dataValue);
            final AggregateActionResponse aggregateActionResponse = percentSamplerAggregateAction.handleEvent(testEvent, aggregateActionInput);
            Event receivedEvent = aggregateActionResponse.getEvent();
            if (receivedEvent != null) {
                assertThat(receivedEvent.get(dataKey, String.class), equalTo(dataValue));
                assertThat(receivedEvent.get(key, String.class), equalTo(value));
                allowedEvents++;
            }
        }
        final AggregateActionOutput actionOutput = percentSamplerAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertTrue(result.isEmpty());
        assertThat(allowedEvents, equalTo((int)(totalEvents * testPercent/100.0)));
    }
}
