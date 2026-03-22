/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MapEntriesProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private MapEntriesProcessorConfig config;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
        lenient().when(config.getSource()).thenReturn("/names");
        lenient().when(config.getKey()).thenReturn("name");
        lenient().when(config.getEffectiveTarget()).thenReturn("/names");
        lenient().when(config.getExcludeNullEmptyValues()).thenReturn(false);
        lenient().when(config.getAppendIfTargetExists()).thenReturn(false);
        lenient().when(config.getMapEntriesWhen()).thenReturn(null);
        lenient().when(config.getTagsOnFailure()).thenReturn(null);
    }

    private MapEntriesProcessor createObjectUnderTest() {
        return new MapEntriesProcessor(pluginMetrics, config, expressionEvaluator);
    }

    private Record<Event> createEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder().withEventType("event").withData(data).build());
    }

    // --- Constructor validation ---

    @Test
    void constructor_with_invalid_map_entries_when_throws_InvalidPluginConfigurationException() {
        final String condition = UUID.randomUUID().toString();
        when(config.getMapEntriesWhen()).thenReturn(condition);
        when(expressionEvaluator.isValidExpressionStatement(condition)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_empty_map_entries_when_throws_InvalidPluginConfigurationException() {
        when(config.getMapEntriesWhen()).thenReturn("");
        when(expressionEvaluator.isValidExpressionStatement("")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_whitespace_map_entries_when_throws_InvalidPluginConfigurationException() {
        when(config.getMapEntriesWhen()).thenReturn("   ");
        when(expressionEvaluator.isValidExpressionStatement("   ")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_empty_target_throws_InvalidPluginConfigurationException() {
        when(config.getTarget()).thenReturn("");

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_empty_string_in_tags_on_failure_throws_InvalidPluginConfigurationException() {
        when(config.getTagsOnFailure()).thenReturn(List.of(""));

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_with_null_element_in_tags_on_failure_throws_InvalidPluginConfigurationException() {
        final List<String> tags = new ArrayList<>();
        tags.add(null);
        when(config.getTagsOnFailure()).thenReturn(tags);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    // --- Basic wrapping ---

    @Test
    void doExecute_with_string_array_wraps_each_element_into_object_in_place() {
        final Record<Event> record = createEvent(Map.of("names", Arrays.asList("alpha", "beta")));
        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/names", List.class);
        assertThat(output.size(), is(2));
        assertThat(output.get(0), equalTo(Map.of("name", "alpha")));
        assertThat(output.get(1), equalTo(Map.of("name", "beta")));
    }

    @Test
    void doExecute_with_separate_target_writes_wrapped_objects_and_preserves_source() {
        when(config.getEffectiveTarget()).thenReturn("/agents");

        final Record<Event> record = createEvent(Map.of("names", Arrays.asList("alpha", "beta")));
        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = result.get(0).getData();
        final List<?> source = event.get("/names", List.class);
        assertThat(source, equalTo(Arrays.asList("alpha", "beta")));
        final List<Map<String, Object>> target = event.get("/agents", List.class);
        assertThat(target.size(), is(2));
        assertThat(target.get(0), equalTo(Map.of("name", "alpha")));
    }

    @Test
    void doExecute_with_mixed_types_wraps_each_element_preserving_original_type() {
        final Record<Event> record = createEvent(Map.of("values", Arrays.asList("alpha", 42, true)));
        when(config.getSource()).thenReturn("/values");
        when(config.getEffectiveTarget()).thenReturn("/values");

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/values", List.class);
        assertThat(output.size(), is(3));
        assertThat(output.get(0), equalTo(Map.of("name", "alpha")));
        assertThat(output.get(1), equalTo(Map.of("name", 42)));
        assertThat(output.get(2), equalTo(Map.of("name", true)));
    }

    // --- Null and empty handling ---

    @Test
    void doExecute_with_null_elements_wraps_nulls_into_objects_by_default() {
        final List<String> input = new ArrayList<>();
        input.add("alpha");
        input.add(null);
        input.add("beta");
        final Record<Event> record = createEvent(Map.of("names", input));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/names", List.class);
        assertThat(output.size(), is(3));
        assertThat(output.get(1).get("name"), equalTo(null));
    }

    @Test
    void doExecute_with_exclude_enabled_filters_out_null_and_empty_elements() {
        when(config.getExcludeNullEmptyValues()).thenReturn(true);

        final List<String> input = new ArrayList<>();
        input.add("alpha");
        input.add(null);
        input.add("");
        input.add("beta");
        final Record<Event> record = createEvent(Map.of("names", input));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/names", List.class);
        assertThat(output.size(), is(2));
        assertThat(output.get(0), equalTo(Map.of("name", "alpha")));
        assertThat(output.get(1), equalTo(Map.of("name", "beta")));
    }

    @Test
    void doExecute_with_all_null_empty_and_exclude_enabled_leaves_original_list_unchanged() {
        when(config.getExcludeNullEmptyValues()).thenReturn(true);

        final List<String> input = new ArrayList<>();
        input.add(null);
        input.add("");
        final Record<Event> record = createEvent(Map.of("names", input));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Object output = result.get(0).getData().get("/names", Object.class);
        assertThat(output instanceof List, is(true));
        assertThat(((List<?>) output).size(), is(2));
    }

    // --- Skip conditions ---

    @Test
    void doExecute_with_missing_source_key_skips_event_and_leaves_it_unchanged() {
        final Record<Event> record = createEvent(Map.of("other", "value"));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        assertThat(result.get(0).getData().containsKey("names"), is(false));
    }

    @Test
    void doExecute_with_non_list_source_skips_event_and_preserves_original_value() {
        final Record<Event> record = createEvent(Map.of("names", "not-a-list"));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        assertThat(result.get(0).getData().get("/names", String.class), equalTo("not-a-list"));
    }

    @Test
    void doExecute_with_empty_source_list_leaves_empty_list_unchanged() {
        final Record<Event> record = createEvent(Map.of("names", Collections.emptyList()));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<?> output = result.get(0).getData().get("/names", List.class);
        assertThat(output.isEmpty(), is(true));
    }

    @Test
    void doExecute_with_false_condition_skips_event_and_preserves_original_array() {
        final String condition = "/type == \"tagged\"";
        when(config.getMapEntriesWhen()).thenReturn(condition);
        when(expressionEvaluator.isValidExpressionStatement(condition)).thenReturn(true);

        final Record<Event> record = createEvent(Map.of("names", Arrays.asList("alpha"), "type", "untagged"));
        when(expressionEvaluator.evaluateConditional(condition, record.getData())).thenReturn(false);

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<?> output = result.get(0).getData().get("/names", List.class);
        assertThat(output, equalTo(Arrays.asList("alpha")));
    }

    // --- Append mode ---

    @Test
    void doExecute_with_append_enabled_merges_new_entries_into_existing_target_list() {
        when(config.getEffectiveTarget()).thenReturn("/result");
        when(config.getAppendIfTargetExists()).thenReturn(true);

        final Map<String, Object> data = new java.util.HashMap<>();
        data.put("names", Arrays.asList("alpha"));
        data.put("result", new ArrayList<>(Arrays.asList(Map.of("name", "existing"))));
        final Record<Event> record = createEvent(data);

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/result", List.class);
        assertThat(output.size(), is(2));
        assertThat(output.get(0), equalTo(Map.of("name", "existing")));
        assertThat(output.get(1), equalTo(Map.of("name", "alpha")));
    }

    @Test
    void doExecute_with_append_enabled_and_missing_target_creates_new_target_list() {
        when(config.getEffectiveTarget()).thenReturn("/result");
        when(config.getAppendIfTargetExists()).thenReturn(true);

        final Record<Event> record = createEvent(Map.of("names", Arrays.asList("alpha")));

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/result", List.class);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), equalTo(Map.of("name", "alpha")));
    }

    @Test
    void doExecute_with_append_enabled_and_non_list_target_skips_and_preserves_target() {
        when(config.getEffectiveTarget()).thenReturn("/result");
        when(config.getAppendIfTargetExists()).thenReturn(true);

        final Map<String, Object> data = new java.util.HashMap<>();
        data.put("names", Arrays.asList("alpha"));
        data.put("result", "not-a-list");
        final Record<Event> record = createEvent(data);

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        assertThat(result.get(0).getData().get("/result", String.class), equalTo("not-a-list"));
    }

    // --- Overwrite mode ---

    @Test
    void doExecute_with_existing_target_overwrites_with_wrapped_objects_by_default() {
        when(config.getEffectiveTarget()).thenReturn("/result");

        final Map<String, Object> data = new java.util.HashMap<>();
        data.put("names", Arrays.asList("alpha"));
        data.put("result", Arrays.asList("old"));
        final Record<Event> record = createEvent(data);

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final List<Map<String, Object>> output = result.get(0).getData().get("/result", List.class);
        assertThat(output.size(), is(1));
        assertThat(output.get(0), equalTo(Map.of("name", "alpha")));
    }

    // --- Multiple records ---

    @Test
    void doExecute_with_multiple_records_wraps_only_matching_and_leaves_non_matching_unchanged() {
        final String condition = "/type == \"users\"";
        when(config.getMapEntriesWhen()).thenReturn(condition);
        when(expressionEvaluator.isValidExpressionStatement(condition)).thenReturn(true);

        final Record<Event> matchingRecord = createEvent(new java.util.HashMap<>(Map.of(
                "names", Arrays.asList("alpha", "beta"), "type", "users")));
        final Record<Event> nonMatchingRecord = createEvent(new java.util.HashMap<>(Map.of(
                "names", Arrays.asList("gamma"), "type", "other")));

        when(expressionEvaluator.evaluateConditional(condition, matchingRecord.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(condition, nonMatchingRecord.getData())).thenReturn(false);

        final List<Record<Event>> result = (List<Record<Event>>) createObjectUnderTest()
                .doExecute(Arrays.asList(matchingRecord, nonMatchingRecord));

        final List<Map<String, Object>> matchedOutput = result.get(0).getData().get("/names", List.class);
        assertThat(matchedOutput.size(), is(2));
        assertThat(matchedOutput.get(0), equalTo(Map.of("name", "alpha")));

        final List<?> unmatchedOutput = result.get(1).getData().get("/names", List.class);
        assertThat(unmatchedOutput, equalTo(Arrays.asList("gamma")));
    }
}
