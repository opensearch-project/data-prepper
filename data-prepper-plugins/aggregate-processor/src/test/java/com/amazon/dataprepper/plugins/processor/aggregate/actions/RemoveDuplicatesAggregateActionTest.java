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

        removeDuplicatesAggregateAction = new RemoveDuplicatesAggregateAction();
    }

    @Test
    void handleEvent_with_null_groupState_returns_expected_AggregateResponse() {
        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, null);

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(true));
        assertThat(aggregateActionResponse.getEvent().get(), equalTo(testEvent));
        assertThat(aggregateActionResponse.isCloseWindowNow(), equalTo(false));
    }

    @Test
    void handleEvent_with_non_null_groupState_returns_expected_AggregateResponse() {
        final AggregateActionResponse aggregateActionResponse = removeDuplicatesAggregateAction.handleEvent(testEvent, Collections.emptyMap());

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(aggregateActionResponse.isCloseWindowNow(), equalTo(false));
    }

    @Test
    void concludeGroup_with_null_groupState_returns_empty_Optional() {
        final Optional<Event> result = removeDuplicatesAggregateAction.concludeGroup(null);

        assertThat(result.isPresent(), equalTo(false));
    }

    @Test
    void concludeGroup_with_non_null_groupState_returns_empty_Optional() {
        final Optional<Event> result = removeDuplicatesAggregateAction.concludeGroup(Collections.emptyMap());

        assertThat(result.isPresent(), equalTo(false));
    }
}
