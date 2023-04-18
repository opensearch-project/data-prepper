/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

public class AppendAggregateActionTest {

    private AggregateAction appendAggregateAction;
    private List<Event> events;
    private List<Map<String, Object>> eventMaps;

    @BeforeEach
    void setup() {
        events = new ArrayList<>();
        eventMaps = new ArrayList<>();

        // { "firstString": "firstEventString", "firstArray": [1, 2, 3], "firstNumber": 1, "matchingNumber": 10, "matchingNumberEqual": 38947, "matchingStringEqual": "equalString", "matchingNumberArray": [20,21,22], "matchingNumberArrayEqual": [20,21,22], "matchingString": "StringFromFirstEvent", "matchingStringArray": ["String1", "String2"],  "matchingDeepArray": [[30,31,32]]}
        final Map<String, Object> firstEventMap = new HashMap<>();
        firstEventMap.put("firstString", "firstEventString");
        firstEventMap.put("firstArray", Arrays.asList(1, 2, 3));
        firstEventMap.put("matchingNumber", 10);
        firstEventMap.put("matchingNumberEqual", 38947);
        firstEventMap.put("matchingNumberArray", Arrays.asList(20, 21, 22));
        firstEventMap.put("matchingNumberArrayEqual", Arrays.asList(20, 21, 22));
        firstEventMap.put("matchingString", "StringFromFirstEvent");
        firstEventMap.put("matchingStringEqual", "equalString");
        firstEventMap.put("matchingStringArray", Arrays.asList("String1", "String2"));
        firstEventMap.put("matchingDeepArray", Arrays.asList(Arrays.asList(30, 31, 32)));
        eventMaps.add(firstEventMap);
        events.add(buildEventFromMap(firstEventMap));

        // { "secondString": "secondEventString", "secondArray": [4, 5, 6], "secondNumber": 2, "matchingNumber": 11, "matchingNumberEqual": 38947, "matchingStringEqual": "equalString", "matchingNumberArray": [23,24,25], "matchingNumberArrayEqual": [20,21,22], "matchingString": "StringFromSecondEvent", "matchingStringArray": ["String3", "String4"], "matchingDeepArray": [[30,31,32]]}
        final Map<String, Object> secondEventMap = new HashMap<>();
        secondEventMap.put("secondString", "secondEventString");
        secondEventMap.put("secondArray", Arrays.asList(4, 5, 6));
        secondEventMap.put("secondNumber", 2);
        secondEventMap.put("matchingNumber", 11);
        secondEventMap.put("matchingNumberEqual", 38947);
        secondEventMap.put("matchingNumberArray", Arrays.asList(23, 24, 25));
        secondEventMap.put("matchingNumberArrayEqual", Arrays.asList(20, 21, 22));
        secondEventMap.put("matchingString", "StringFromSecondEvent");
        secondEventMap.put("matchingStringEqual", "equalString");
        secondEventMap.put("matchingStringArray", Arrays.asList("String3", "String4"));
        secondEventMap.put("matchingDeepArray", Arrays.asList(Arrays.asList(30, 31, 32)));
        eventMaps.add(secondEventMap);
        events.add(buildEventFromMap(secondEventMap));
    }

    private Event buildEventFromMap(final Map<String, Object> eventMap) {
        return JacksonEvent.builder().withEventType("event").withData(eventMap).build();
    }

    private AggregateAction createObjectUnderTest(AppendAggregateActionConfig config) {
        return new AppendAggregateAction(config);
    }

    @Test
    void handleEvent_with_empty_group_state_should_return_correct_AggregateResponse_and_add_event_to_groupState() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add(UUID.randomUUID().toString());
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());

        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);

        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(events.get(0).toMap()));
    }

    private void consumeEvent(Event event, GroupState groupState, List<String> keysToAppend) {
        event.toMap().forEach((key, value) -> {
            if (keysToAppend == null || keysToAppend.isEmpty() || keysToAppend.contains(key)) {
                Object valueFromGroupState = groupState.getOrDefault(key, value);
                if (valueFromGroupState instanceof List) {
                    if (value instanceof List) {
                        ((List) valueFromGroupState).addAll((List) value);
                    } else {
                        ((List) valueFromGroupState).add(value);
                    }
                } else {
                    if (!Objects.equals(value, valueFromGroupState)) {
                        groupState.put(key, Arrays.asList(valueFromGroupState, value));
                    }
                }
            }
        });
    }

    @Test
    void handleEvent_with_non_empty_groupState_emptyKeysToAppend_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_non_empty_groupState_nonExistentField_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add(UUID.randomUUID().toString());
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }


    @Test
    void handleEvent_with_numberField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingNumber");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_stringField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingString");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_equalNumberField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingNumberEqual");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_equalStringField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingStringEqual");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_numberArrayField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingNumberArray");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void handleEvent_with_stringArrayField_groupState_should_combine_Event_with_groupState_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add("matchingStringArray");
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState expectedGroupState = new AggregateActionTestUtils.TestGroupState();
        expectedGroupState.putAll(events.get(0).toMap());
        consumeEvent(events.get(1), expectedGroupState, testKeysToAppend);
        // Handle first event
        appendAggregateAction.handleEvent(events.get(0), aggregateActionInput);
        // Handle second event
        final AggregateActionResponse aggregateActionResponse = appendAggregateAction.handleEvent(events.get(1), aggregateActionInput);
        assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        assertThat(aggregateActionInput.getGroupState(), equalTo(expectedGroupState));
    }

    @Test
    void concludeGroup_should_return_groupState_As_An_Event_correctly() throws NoSuchFieldException, IllegalAccessException {
        AppendAggregateActionConfig appendAggregateActionConfig = new AppendAggregateActionConfig();
        final List<String> testKeysToAppend = new ArrayList<>();
        testKeysToAppend.add(UUID.randomUUID().toString());
        setField(AppendAggregateActionConfig.class, appendAggregateActionConfig, "keysToAppend", testKeysToAppend);
        appendAggregateAction = createObjectUnderTest(appendAggregateActionConfig);

        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Collections.emptyMap());
        final GroupState groupState = aggregateActionInput.getGroupState();
        for (final Map<String, Object> eventMap : eventMaps) {
            groupState.putAll(eventMap);
        }

        final AggregateActionOutput actionOutput = appendAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getMetadata().getEventType(), equalTo(AppendAggregateAction.EVENT_TYPE));
        assertThat(result.get(0).toMap(), equalTo(groupState));
    }
}
