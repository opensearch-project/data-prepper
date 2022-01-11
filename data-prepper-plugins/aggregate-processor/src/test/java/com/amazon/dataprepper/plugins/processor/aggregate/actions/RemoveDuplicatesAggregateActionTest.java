/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate.actions;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class RemoveDuplicatesAggregateActionTest {
    private AggregateAction removeDuplicatesAggregateAction;
    private Event testEvent;

    @BeforeEach
    void setup() {
        testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .build();
    }

    private AggregateAction createObjectUnderTest() {
        return new RemoveDuplicatesAggregateAction();
    }

    @Test
    void handleEvent_with_empty_groupState_returns_expected_AggregateResponse_and_modifies_groupState() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final Map<Object, Object> groupState = new HashMap<>();
        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, groupState);

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(true));
        assertThat(aggregateActionResponse.getEvent().get(), equalTo(testEvent));
        assertThat(groupState, equalTo(testEvent.toMap()));
    }

    @Test
    void handleEvent_with_non_empty_groupState_returns_expected_AggregateResponse_and_does_not_modify_groupState() {
        removeDuplicatesAggregateAction = createObjectUnderTest();

        final Map<Object, Object> groupState = new HashMap<>();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        final Map<Object, Object> tempGroupState = new HashMap<>(groupState);
        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, groupState);

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(groupState, equalTo(tempGroupState));
    }

    @Test
    void concludeGroup_with_empty_groupState_returns_empty_Optional() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final Optional<Event> result = removeDuplicatesAggregateAction.concludeGroup(Collections.emptyMap());

        assertThat(result.isPresent(), equalTo(false));
    }

    @Test
    void concludeGroup_with_non_empty_groupState_returns_empty_Optional() {
        removeDuplicatesAggregateAction = createObjectUnderTest();
        final Map<Object, Object> groupState = new HashMap<>();
        groupState.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Optional<Event> result = removeDuplicatesAggregateAction.concludeGroup(groupState);

        assertThat(result.isPresent(), equalTo(false));
    }
}
