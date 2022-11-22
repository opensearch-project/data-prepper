/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

@ExtendWith(MockitoExtension.class)
public class CountAggregateActionTest {
    AggregateActionInput aggregateActionInput;

    private AggregateAction countAggregateAction;

    private AggregateAction createObjectUnderTest() {
        return new CountAggregateAction();
    }

    @Test
    void TestCountAggregation() {
        countAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput();
        final GroupState groupState = aggregateActionInput.getGroupState();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(key, value))
                .build();
        final int testCount = 10;
        for (int i = 0; i < testCount; i++) { 
            final AggregateActionResponse aggregateActionResponse = countAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }
        groupState.put(key, value);

        final Optional<Event> result = countAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(result.isPresent(), equalTo(true));
        Map<String, Object> expectedEventMap = new HashMap<>() {{ put(key, value); }};
        expectedEventMap.put(CountAggregateAction.COUNTKEY, testCount);
        assertEquals(expectedEventMap, result.get().toMap());
    }
}
