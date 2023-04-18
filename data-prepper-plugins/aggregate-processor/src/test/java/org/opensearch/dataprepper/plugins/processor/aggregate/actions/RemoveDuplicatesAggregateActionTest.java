/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RemoveDuplicatesAggregateActionTest {
    private AggregateAction removeDuplicatesAggregateAction;
    private Event testEvent;
    private GroupState expectedGroupState;

    @BeforeEach
    void setup() {
        testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();

        expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.put(RemoveDuplicatesAggregateAction.GROUP_STATE_HAS_EVENT, true);
    }

    private AggregateAction createObjectUnderTest() {
        return new RemoveDuplicatesAggregateAction();
    }

    @Test
    void handleEvent_with_empty_groupState_returns_expected_AggregateResponse_and_modifies_groupState() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();
        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, aggregateActionInput);

        assertThat(aggregateActionResponse.getEvent(), equalTo(testEvent));
        assertThat(groupState, equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_non_empty_groupState_returns_expected_AggregateResponse_and_does_not_modify_groupState() {
        removeDuplicatesAggregateAction = createObjectUnderTest();

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();
        groupState.put(RemoveDuplicatesAggregateAction.GROUP_STATE_HAS_EVENT, true);

        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, aggregateActionInput);

        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(groupState, equalTo(expectedGroupState));
    }

    @Test
    void concludeGroup_with_empty_groupState_returns_empty_List() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final AggregateActionOutput actionOutput = removeDuplicatesAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();

        assertTrue(result.isEmpty());
    }

    @Test
    void concludeGroup_with_non_empty_groupState_returns_empty_List() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final AggregateActionOutput actionOutput = removeDuplicatesAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();

        assertTrue(result.isEmpty());
    }
}
