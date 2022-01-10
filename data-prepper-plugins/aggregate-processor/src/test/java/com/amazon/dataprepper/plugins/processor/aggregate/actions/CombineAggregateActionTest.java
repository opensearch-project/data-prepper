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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class CombineAggregateActionTest {
    private AggregateAction combineAggregateAction;
    private List<Event> events;
    private List<Map<String, Object>> eventMaps;

    @BeforeEach
    void setup() {
        events = new ArrayList<>();
        eventMaps = new ArrayList<>();
        combineAggregateAction = new CombineAggregateAction();

        final Map<String, Object> firstEventMap = new HashMap<>();
        firstEventMap.put("string", "firstEventString");
        firstEventMap.put("array", Arrays.asList(1, 2, 3));
        eventMaps.add(firstEventMap);
        events.add(buildEventFromMap(firstEventMap));

        final Map<String, Object> secondEventMap = new HashMap<>();
        secondEventMap.put("string", "secondEventString");
        secondEventMap.put("number", 2);
        eventMaps.add(secondEventMap);
        events.add(buildEventFromMap(secondEventMap));

        final Map<String, Object> thirdEventMap = new HashMap<>();
        thirdEventMap.put("string", "thirdEventString");
        thirdEventMap.put("double", 2.1);
        eventMaps.add(thirdEventMap);
        events.add(buildEventFromMap(thirdEventMap));

    }

    private Event buildEventFromMap(final Map<String, Object> eventMap) {
        return JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
    }

    @Test
    void handleEvent_with_null_group_state_should_return_correct_AggregateResponse() {
        final AggregateActionResponse aggregateActionResponse = combineAggregateAction.handleEvent(events.get(0), null);

        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(aggregateActionResponse.isCloseWindowNow(), equalTo(false));
    }

    @Test
    void combining_with_single_event_should_result_in_that_same_event_being_returned_on_concludeGroup() {
        final Map<Object, Object> groupState = new HashMap<>();
        final AggregateActionResponse aggregateActionResponse = combineAggregateAction.handleEvent(events.get(0), groupState);
        assertThat(aggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(aggregateActionResponse.isCloseWindowNow(), equalTo(false));

        final Optional<Event> result = combineAggregateAction.concludeGroup(groupState);
        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().toMap(), equalTo(events.get(0).toMap()));
        assertThat(result.get().getMetadata().getEventType(), equalTo(CombineAggregateAction.COMBINED_EVENT_TYPE));
    }

    @Test
    void combining_with_multiple_events_should_combine_into_group_state_correctly_and_return_correct_event_on_concludeGroup() {
        final Map<Object, Object> groupState = new HashMap<>();
        final Map<String, Object> expectedResult = new HashMap<>();
        for (final Map<String, Object> eventMap : eventMaps) {
            expectedResult.putAll(eventMap);
        }

        final AggregateActionResponse firstAggregateActionResponse = combineAggregateAction.handleEvent(events.get(0), groupState);
        assertThat(firstAggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(firstAggregateActionResponse.isCloseWindowNow(), equalTo(false));

        final AggregateActionResponse secondAggregateActionResponse = combineAggregateAction.handleEvent(events.get(1), groupState);
        assertThat(secondAggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(secondAggregateActionResponse.isCloseWindowNow(), equalTo(false));

        final AggregateActionResponse thirdAggregateActionResponse = combineAggregateAction.handleEvent(events.get(2), groupState);
        assertThat(thirdAggregateActionResponse.getEvent().isPresent(), equalTo(false));
        assertThat(thirdAggregateActionResponse.isCloseWindowNow(), equalTo(false));

        final Optional<Event> result = combineAggregateAction.concludeGroup(groupState);
        assertThat(result.isPresent(), equalTo(true));
        assertThat(result.get().getMetadata().getEventType(), equalTo(CombineAggregateAction.COMBINED_EVENT_TYPE));
        assertThat(result.get().toMap(), equalTo(expectedResult));
    }
}
