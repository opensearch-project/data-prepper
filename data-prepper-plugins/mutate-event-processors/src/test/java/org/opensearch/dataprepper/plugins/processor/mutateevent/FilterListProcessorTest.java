/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FilterListProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private FilterListProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private static final String KEEP_WHEN_EXPRESSION = "/type == \"cve\"";
    private static final String SOURCE_KEY = "identifiers";

    @BeforeEach
    void setUp() {
        lenient().when(mockConfig.getSource()).thenReturn(SOURCE_KEY);
        lenient().when(mockConfig.getTarget()).thenReturn(SOURCE_KEY);
        lenient().when(mockConfig.getKeepWhen()).thenReturn(KEEP_WHEN_EXPRESSION);
        lenient().when(mockConfig.getFilterListWhen()).thenReturn(null);
        lenient().when(mockConfig.getTagsOnFailure()).thenReturn(null);
        lenient().when(expressionEvaluator.isValidExpressionStatement(KEEP_WHEN_EXPRESSION)).thenReturn(true);
        lenient().doCallRealMethod().when(mockConfig).validateExpressions(expressionEvaluator);
    }

    @Test
    void invalid_keep_when_throws_InvalidPluginConfigurationException() {
        final String invalidExpression = UUID.randomUUID().toString();
        when(mockConfig.getKeepWhen()).thenReturn(invalidExpression);
        when(expressionEvaluator.isValidExpressionStatement(invalidExpression)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void invalid_filter_list_when_throws_InvalidPluginConfigurationException() {
        final String filterListWhen = UUID.randomUUID().toString();
        when(mockConfig.getFilterListWhen()).thenReturn(filterListWhen);
        when(expressionEvaluator.isValidExpressionStatement(filterListWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void test_filter_keeps_matching_elements_in_place() {
        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0).get("id"), is("CVE-1"));
        assertThat(filteredList.get(1).get("id"), is("CVE-2"));
    }

    @Test
    void test_filter_writes_to_different_target() {
        final String targetKey = "cve_identifiers";
        when(mockConfig.getTarget()).thenReturn(targetKey);

        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();

        // Original source should remain unchanged
        final List<Map<String, Object>> originalList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(originalList.size(), is(3));

        // Target should have filtered list
        final List<Map<String, Object>> filteredList = resultEvent.get(targetKey, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0).get("id"), is("CVE-1"));
        assertThat(filteredList.get(1).get("id"), is("CVE-2"));
    }

    @Test
    void test_filter_writes_to_nested_target() {
        final String nestedTarget = "vulnerability/cve_ids";
        when(mockConfig.getTarget()).thenReturn(nestedTarget);

        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(nestedTarget, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
    }

    @Test
    void test_filter_removes_all_elements_when_none_match() {
        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(false);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(0));
    }

    @Test
    void test_filter_keeps_all_elements_when_all_match() {
        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(true);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(3));
    }

    @Test
    void test_filter_with_empty_source_list() {
        final FilterListProcessor processor = createObjectUnderTest();

        final Map<String, Object> data = Map.of(SOURCE_KEY, List.of());
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(0));
    }

    @Test
    void test_filter_with_null_source_list_is_noop() {
        final FilterListProcessor processor = createObjectUnderTest();

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, null);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
    }

    @Test
    void test_filter_with_missing_source_key_is_noop() {
        final FilterListProcessor processor = createObjectUnderTest();

        final Map<String, Object> data = Map.of("other_key", "value");
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        // Event should remain unchanged since source key doesn't exist
        assertThat(resultEvent.toMap(), equalTo(testRecord.getData().toMap()));
    }

    @Test
    void test_filter_skipped_when_filter_list_when_is_false() {
        final String filterListWhen = UUID.randomUUID().toString();
        when(mockConfig.getFilterListWhen()).thenReturn(filterListWhen);
        when(expressionEvaluator.isValidExpressionStatement(filterListWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(filterListWhen, testRecord.getData())).thenReturn(false);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        // Event should remain unchanged
        assertThat(resultEvent.toMap(), equalTo(testRecord.getData().toMap()));
    }

    @Test
    void test_filter_runs_when_filter_list_when_is_true() {
        final String filterListWhen = UUID.randomUUID().toString();
        when(mockConfig.getFilterListWhen()).thenReturn(filterListWhen);
        when(expressionEvaluator.isValidExpressionStatement(filterListWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(filterListWhen, testRecord.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(false);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(0));
    }

    @Test
    void test_filter_with_nested_source_path() {
        final String nestedSource = "vulnerability/identifiers";
        when(mockConfig.getSource()).thenReturn(nestedSource);
        when(mockConfig.getTarget()).thenReturn(nestedSource);

        final FilterListProcessor processor = createObjectUnderTest();

        final List<Map<String, Object>> identifiers = new ArrayList<>();
        identifiers.add(Map.of("id", "CVE-1", "type", "cve"));
        identifiers.add(Map.of("id", "CWE-1", "type", "cwe"));

        final Map<String, Object> data = Map.of("vulnerability", Map.of("identifiers", identifiers));
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event e = invocation.getArgument(1);
                    return "cve".equals(e.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(nestedSource, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(1));
        assertThat(filteredList.get(0).get("id"), is("CVE-1"));
    }

    @Test
    void test_filter_with_mixed_map_and_primitive_elements() {
        final FilterListProcessor processor = createObjectUnderTest();

        final List<Object> mixedList = new ArrayList<>();
        mixedList.add(Map.of("id", "CVE-1", "type", "cve"));
        mixedList.add("a-string");
        mixedList.add(Map.of("id", "CVE-2", "type", "cve"));

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, mixedList);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(true);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Object> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(3));
    }

    @Test
    void test_filter_primitive_string_list() {
        final String keepWhen = "/value != \"\"";
        when(mockConfig.getKeepWhen()).thenReturn(keepWhen);
        when(expressionEvaluator.isValidExpressionStatement(keepWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();

        final List<Object> stringList = new ArrayList<>();
        stringList.add("hello");
        stringList.add("");
        stringList.add("world");

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, stringList);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(keepWhen), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event e = invocation.getArgument(1);
                    Object val = e.get("value", Object.class);
                    return val != null && !"".equals(val);
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final List<Object> filteredList = resultRecords.get(0).getData().get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0), is("hello"));
        assertThat(filteredList.get(1), is("world"));
    }

    @Test
    void test_filter_primitive_number_list() {
        final String keepWhen = "/value > 0";
        when(mockConfig.getKeepWhen()).thenReturn(keepWhen);
        when(expressionEvaluator.isValidExpressionStatement(keepWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();

        final List<Object> numberList = new ArrayList<>();
        numberList.add(10);
        numberList.add(-5);
        numberList.add(0);
        numberList.add(42);

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, numberList);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(keepWhen), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event e = invocation.getArgument(1);
                    Object val = e.get("value", Object.class);
                    return val instanceof Number && ((Number) val).intValue() > 0;
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final List<Object> filteredList = resultRecords.get(0).getData().get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0), is(10));
        assertThat(filteredList.get(1), is(42));
    }

    @Test
    void test_filter_primitive_list_with_nulls() {
        final String keepWhen = "/value != null";
        when(mockConfig.getKeepWhen()).thenReturn(keepWhen);
        when(expressionEvaluator.isValidExpressionStatement(keepWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();

        final List<Object> listWithNulls = new ArrayList<>();
        listWithNulls.add("keep");
        listWithNulls.add(null);
        listWithNulls.add(42);

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, listWithNulls);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(keepWhen), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event e = invocation.getArgument(1);
                    return e.get("value", Object.class) != null;
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final List<Object> filteredList = resultRecords.get(0).getData().get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0), is("keep"));
        assertThat(filteredList.get(1), is(42));
    }

    @Test
    void test_filter_continues_processing_when_expression_throws_for_one_element() {
        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(true)
                .thenThrow(new RuntimeException("expression error"))
                .thenReturn(true);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
    }

    @Test
    void test_filter_processes_multiple_records() {
        final FilterListProcessor processor = createObjectUnderTest();

        final Record<Event> record1 = createTestRecord();
        final Record<Event> record2 = createTestRecord();

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(List.of(record1, record2));

        assertThat(resultRecords.size(), is(2));
        for (final Record<Event> resultRecord : resultRecords) {
            final List<Map<String, Object>> filteredList = resultRecord.getData().get(SOURCE_KEY, List.class);
            assertThat(filteredList.size(), is(2));
        }
    }

    @Test
    void test_filter_list_when_true_and_keep_when_selectively_filters() {
        final String filterListWhen = "/env == \"production\"";
        when(mockConfig.getFilterListWhen()).thenReturn(filterListWhen);
        when(expressionEvaluator.isValidExpressionStatement(filterListWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(filterListWhen, testRecord.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList, is(notNullValue()));
        assertThat(filteredList.size(), is(2));
        assertThat(filteredList.get(0).get("id"), is("CVE-1"));
        assertThat(filteredList.get(1).get("id"), is("CVE-2"));
    }

    @Test
    void test_multiple_records_with_different_filter_list_when_outcomes() {
        final String filterListWhen = "/process == true";
        when(mockConfig.getFilterListWhen()).thenReturn(filterListWhen);
        when(expressionEvaluator.isValidExpressionStatement(filterListWhen)).thenReturn(true);

        final FilterListProcessor processor = createObjectUnderTest();

        // Record 1: filter_list_when passes
        final Record<Event> record1 = createTestRecord();
        // Record 2: filter_list_when fails
        final Map<String, Object> data2 = new HashMap<>();
        final List<Map<String, Object>> list2 = new ArrayList<>();
        list2.add(Map.of("id", "KEEP-1", "type", "cve"));
        list2.add(Map.of("id", "KEEP-2", "type", "cwe"));
        data2.put(SOURCE_KEY, list2);
        final Event event2 = JacksonEvent.builder().withData(data2).withEventType("event").build();
        final Record<Event> record2 = new Record<>(event2);

        when(expressionEvaluator.evaluateConditional(filterListWhen, record1.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(filterListWhen, record2.getData())).thenReturn(false);
        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenAnswer(invocation -> {
                    Event event = invocation.getArgument(1);
                    return "cve".equals(event.get("type", String.class));
                });

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(List.of(record1, record2));

        assertThat(resultRecords.size(), is(2));

        // Record 1: filtered
        final List<Map<String, Object>> filteredList1 = resultRecords.get(0).getData().get(SOURCE_KEY, List.class);
        assertThat(filteredList1.size(), is(2));
        assertThat(filteredList1.get(0).get("id"), is("CVE-1"));

        // Record 2: untouched
        final List<Map<String, Object>> untouchedList = resultRecords.get(1).getData().get(SOURCE_KEY, List.class);
        assertThat(untouchedList.size(), is(2));
        assertThat(untouchedList.get(0).get("id"), is("KEEP-1"));
        assertThat(untouchedList.get(1).get("id"), is("KEEP-2"));
    }

    @Test
    void test_filter_with_source_not_a_list_adds_tags_on_failure() {
        final List<String> testTags = List.of("tag1", "tag2");
        when(mockConfig.getTagsOnFailure()).thenReturn(testTags);

        final FilterListProcessor processor = createObjectUnderTest();

        final Map<String, Object> data = Map.of(SOURCE_KEY, "not-a-list");
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        assertThat(resultEvent.getMetadata().getTags(), is(new HashSet<>(testTags)));
    }

    @Test
    void test_filter_with_single_element_list() {
        final FilterListProcessor processor = createObjectUnderTest();

        final List<Map<String, Object>> singleElementList = List.of(Map.of("id", "CVE-1", "type", "cve"));
        final Map<String, Object> data = Map.of(SOURCE_KEY, singleElementList);
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(true);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        final List<Map<String, Object>> filteredList = resultEvent.get(SOURCE_KEY, List.class);
        assertThat(filteredList.size(), is(1));
        assertThat(filteredList.get(0).get("id"), is("CVE-1"));
    }

    @Test
    void test_isReadyForShutdown_returns_true() {
        final FilterListProcessor processor = createObjectUnderTest();
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void test_outer_catch_block_adds_tags_on_failure_when_unexpected_exception_occurs() {
        final List<String> testTags = List.of("filter_list_failure");
        when(mockConfig.getTagsOnFailure()).thenReturn(testTags);

        final FilterListProcessor processor = createObjectUnderTest();

        final List<Map<String, Object>> identifiers = new ArrayList<>();
        identifiers.add(Map.of("id", "CVE-1", "type", "cve"));

        final Map<String, Object> data = new HashMap<>();
        data.put(SOURCE_KEY, identifiers);
        final Event event = spy(JacksonEvent.builder().withData(data).withEventType("event").build());
        doThrow(new RuntimeException("unexpected error")).when(event).put(any(String.class), any());
        final Record<Event> testRecord = new Record<>(event);

        when(expressionEvaluator.evaluateConditional(eq(KEEP_WHEN_EXPRESSION), any(Event.class)))
                .thenReturn(true);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        assertThat(resultEvent.getMetadata().getTags(), is(new HashSet<>(testTags)));
    }

    @Test
    void test_tags_not_added_when_tags_on_failure_is_null_and_source_is_not_a_list() {
        final FilterListProcessor processor = createObjectUnderTest();

        final Map<String, Object> data = Map.of(SOURCE_KEY, "not-a-list");
        final Event event = JacksonEvent.builder().withData(data).withEventType("event").build();
        final Record<Event> testRecord = new Record<>(event);

        final List<Record<Event>> resultRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecords.size(), is(1));
        final Event resultEvent = resultRecords.get(0).getData();
        assertThat(resultEvent.getMetadata().getTags().isEmpty(), is(true));
    }

    private FilterListProcessor createObjectUnderTest() {
        return new FilterListProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Record<Event> createTestRecord() {
        final List<Map<String, Object>> identifiers = new ArrayList<>();
        identifiers.add(Map.of("id", "CVE-1", "type", "cve"));
        identifiers.add(Map.of("id", "CVE-2", "type", "cve"));
        identifiers.add(Map.of("id", "CWE-1", "type", "cwe"));

        final Map<String, Object> data = Map.of(SOURCE_KEY, identifiers);
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }
}
