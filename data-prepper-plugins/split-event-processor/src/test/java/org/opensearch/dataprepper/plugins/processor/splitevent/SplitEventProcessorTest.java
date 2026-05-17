/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.splitevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SplitEventProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SplitEventProcessorConfig mockConfig;

    @Mock
    private AcknowledgementSet mockAcknowledgementSet;

    private SplitEventProcessor splitEventProcessor;

    private Record<Event> createTestRecord(final Map<String, Object> data) {
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();

        final DefaultEventHandle eventHandle = (DefaultEventHandle) event.getEventHandle();
        eventHandle.addAcknowledgementSet(mockAcknowledgementSet);
        return new Record<>(event);
    }

    private SplitEventProcessor createNoDelimiterProcessor() {
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn(null);
        return new SplitEventProcessor(pluginMetrics, mockConfig);
    }

    @BeforeEach
    void setup() {
        when(mockConfig.getField()).thenReturn("k1");
        when(mockConfig.getDelimiter()).thenReturn(" ");

        splitEventProcessor = new SplitEventProcessor(pluginMetrics, mockConfig);
    }

    private static Stream<Arguments> provideMaps() {
        return Stream.of(
                Arguments.of(Map.of("k1", "", "k2", "v2")),
                Arguments.of(Map.of("k1", "v1", "k2", "v2")),
                Arguments.of(Map.of("k1", "v1 v2", "k2", "v2")),
                Arguments.of(Map.of("k1", "v1 v2 v3", "k2", "v2"))
        );
    }

    private static Stream<Arguments> provideEmptyOrNullDelimiterCombinations() {
        return Stream.of(
                Arguments.of("", null),
                Arguments.of(null, ""),
                Arguments.of("", "")
        );
    }

    @Test
    void testHappyPathWithSpaceDelimiter() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2")));
    }

    @Test
    void testHappyPathWithSpaceDelimiterForStringWithMultipleSpaces() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1  v2");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(3));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1", "v2")));
    }

    @Test
    void testHappyPathWithSemiColonDelimiter() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1;v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(";");

        final SplitEventProcessor processor = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2")));
    }

    @Test
    void testHappyPathWithSpaceDelimiterRegex() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn("\\s+");

        final SplitEventProcessor processor = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2")));
    }

    @Test
    void testHappyPathWithColonDelimiterRegex() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1:v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn(":");

        final SplitEventProcessor processor = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2")));
    }

    @Test
    void testFailureWithBothDelimiterRegexAndDelimiterDefined() {
        when(mockConfig.getDelimiter()).thenReturn(" ");
        when(mockConfig.getDelimiterRegex()).thenReturn("\\s+");
        assertThrows(IllegalArgumentException.class, () -> new SplitEventProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testNoDelimiterWithArrayFieldSplitsArray() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("a", "b"));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("b"));
    }

    @Test
    void testNoDelimiterWithNonArrayFieldPassesThrough() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "just a string");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("k1", String.class), equalTo("just a string"));
    }

    @Test
    void testNoDelimiterWithIntegerValuePassesThrough() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", 42);
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo(42));
    }

    @Test
    void testDelimiterModeWithNonStringValuePassesThrough() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", 99);
        testData.put("k2", "other");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo(99));
        assertThat(results.get(0).getData().get("k2", String.class), equalTo("other"));
    }

    @ParameterizedTest
    @MethodSource("provideMaps")
    void testSplitEventsBelongToTheSameAcknowledgementSet(final Map<String, Object> inputMap) {
        final Record<Event> testRecord = createTestRecord(inputMap);
        final DefaultEventHandle originalEventHandle = (DefaultEventHandle) testRecord.getData().getEventHandle();
        final AcknowledgementSet originalAcknowledgementSet = originalEventHandle.getAcknowledgementSet();

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(testRecord));

        final Record<Event> lastRecord = editedRecords.get(editedRecords.size() - 1);
        final DefaultEventHandle lastEventHandle = (DefaultEventHandle) lastRecord.getData().getEventHandle();
        assertThat(lastEventHandle.getAcknowledgementSet(), equalTo(originalAcknowledgementSet));
    }

    @Test
    void testSplitEventsWhenNoSplits() {
        final List<Record<Event>> records = new ArrayList<>();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1");
        testData.put("k2", "v2");
        records.add(createTestRecord(testData));

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k1", "");
        testData2.put("k3", "v3");
        records.add(createTestRecord(testData2));

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1", "k2", "v2")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "", "k3", "v3")));
    }

    @Test
    void testSplitEventsWhenOneSplit() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        testData.put("k2", "v3");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1", "k2", "v3")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2", "k2", "v3")));
    }

    @Test
    void testSplitEventsWhenMultipleSplits() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2 v3 v4");
        testData.put("k2", "v5");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords, hasSize(4));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1", "k2", "v5")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2", "k2", "v5")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1", "v3", "k2", "v5")));
        assertThat(editedRecords.get(3).getData().toMap(), equalTo(Map.of("k1", "v4", "k2", "v5")));
    }

    @Test
    void testSplitEventsWhenMultipleSplitsMultipleRecords() {
        final List<Record<Event>> records = new ArrayList<>();

        final Map<String, Object> testData1 = new HashMap<>();
        testData1.put("k1", "v1 v2");
        testData1.put("k2", "v5");
        records.add(createTestRecord(testData1));

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k1", "v1");
        testData2.put("k2", "v3");
        records.add(createTestRecord(testData2));

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords, hasSize(3));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1", "v1", "k2", "v5")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1", "v2", "k2", "v5")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1", "v1", "k2", "v3")));
    }

    @Test
    void testSplitEventsWhenNoKeyPresentInEvent() {
        final List<Record<Event>> records = new ArrayList<>();

        final Map<String, Object> testData1 = new HashMap<>();
        testData1.put("k2", "v5 v6");
        records.add(createTestRecord(testData1));

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k3", "v3 v5");
        records.add(createTestRecord(testData2));

        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords, hasSize(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k2", "v5 v6")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k3", "v3 v5")));
    }

    @Test
    void testIsReadyForShutdown() {
        assertThat(splitEventProcessor.isReadyForShutdown(), equalTo(true));
    }

    @Test
    void testArraySplitWithStringElements() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("a", "b", "c"));
        testData.put("k2", "preserved");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(3));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(0).getData().get("k2", String.class), equalTo("preserved"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("b"));
        assertThat(results.get(2).getData().get("k1", Object.class), equalTo("c"));
    }

    @Test
    void testArraySplitWithObjectElements() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of(Map.of("name", "Alice"), Map.of("name", "Bob")));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("k1", Map.class), equalTo(Map.of("name", "Alice")));
        assertThat(results.get(1).getData().get("k1", Map.class), equalTo(Map.of("name", "Bob")));
    }

    @Test
    void testArraySplitWithSingleElement() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("only"));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("only"));
    }

    @Test
    void testArraySplitWithEmptyArray() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of());
        testData.put("k2", "other");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        final Object k1Value = results.get(0).getData().get("k1", Object.class);
        assertThat(k1Value, instanceOf(List.class));
        assertThat((List<Object>) k1Value, empty());
        assertThat(results.get(0).getData().get("k2", String.class), equalTo("other"));
    }

    @Test
    void testArraySplitWithMixedTypes() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("text", 42, true));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(3));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("text"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo(42));
        assertThat(results.get(2).getData().get("k1", Object.class), equalTo(true));
    }

    @Test
    void testArraySplitWithNestedArrays() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of(List.of(1, 2), List.of(3, 4)));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo(List.of(1, 2)));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo(List.of(3, 4)));
    }

    @Test
    void testArraySplitPreservesOtherFields() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("a", "b"));
        testData.put("host", "server1");
        testData.put("level", "info");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("host", String.class), equalTo("server1"));
        assertThat(results.get(0).getData().get("level", String.class), equalTo("info"));
        assertThat(results.get(1).getData().get("host", String.class), equalTo("server1"));
        assertThat(results.get(1).getData().get("level", String.class), equalTo("info"));
    }

    @Test
    void testArraySplitFieldMissing() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("other", "value");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("other", String.class), equalTo("value"));
    }

    @Test
    void testArraySplitMultipleRecordsMixed() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final List<Record<Event>> records = new ArrayList<>();

        final Map<String, Object> arrayData = new HashMap<>();
        arrayData.put("k1", List.of("x", "y"));
        records.add(createTestRecord(arrayData));

        final Map<String, Object> stringData = new HashMap<>();
        stringData.put("k1", "no split");
        records.add(createTestRecord(stringData));

        final Map<String, Object> arrayData2 = new HashMap<>();
        arrayData2.put("k1", List.of("a", "b", "c"));
        records.add(createTestRecord(arrayData2));

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(records);

        assertThat(results, hasSize(6));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("x"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("y"));
        assertThat(results.get(2).getData().get("k1", String.class), equalTo("no split"));
        assertThat(results.get(3).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(4).getData().get("k1", Object.class), equalTo("b"));
        assertThat(results.get(5).getData().get("k1", Object.class), equalTo("c"));
    }

    @Test
    void testArraySplitAcknowledgementSet() {
        final SplitEventProcessor processor = createNoDelimiterProcessor();

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("a", "b", "c"));
        final Record<Event> record = createTestRecord(testData);

        final DefaultEventHandle originalHandle = (DefaultEventHandle) record.getData().getEventHandle();
        final AcknowledgementSet originalAckSet = originalHandle.getAcknowledgementSet();

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(3));
        final DefaultEventHandle lastHandle = (DefaultEventHandle) results.get(2).getData().getEventHandle();
        assertThat(lastHandle.getAcknowledgementSet(), equalTo(originalAckSet));
    }

    @Test
    void testDelimiterModeWithArrayFieldSplitsArray() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("a", "b", "c"));
        testData.put("k2", "preserved");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(3));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(0).getData().get("k2", String.class), equalTo("preserved"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("b"));
        assertThat(results.get(2).getData().get("k1", Object.class), equalTo("c"));
    }

    @Test
    void testDelimiterModeWithMixedStringAndArrayRecords() {
        final List<Record<Event>> records = new ArrayList<>();

        final Map<String, Object> stringData = new HashMap<>();
        stringData.put("k1", "v1 v2");
        records.add(createTestRecord(stringData));

        final Map<String, Object> arrayData = new HashMap<>();
        arrayData.put("k1", List.of("a", "b"));
        records.add(createTestRecord(arrayData));

        final List<Record<Event>> results = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(results, hasSize(4));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("v1"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("v2"));
        assertThat(results.get(2).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(3).getData().get("k1", Object.class), equalTo("b"));
    }

    @Test
    void testPrepareForShutdownDoesNotThrow() {
        splitEventProcessor.prepareForShutdown();
    }

    @Test
    void testShutdownDoesNotThrow() {
        splitEventProcessor.shutdown();
    }

    @ParameterizedTest
    @MethodSource("provideEmptyOrNullDelimiterCombinations")
    void testEmptyOrNullDelimiterCombinationsEnterArrayMode(final String delimiter, final String delimiterRegex) {
        when(mockConfig.getDelimiter()).thenReturn(delimiter);
        when(mockConfig.getDelimiterRegex()).thenReturn(delimiterRegex);
        final SplitEventProcessor processor = new SplitEventProcessor(pluginMetrics, mockConfig);

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", List.of("x", "y"));
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("x"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("y"));
    }

    @Test
    void testNullFieldValuePassesThrough() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", null);
        testData.put("k2", "preserved");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) splitEventProcessor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData().get("k2", String.class), equalTo("preserved"));
    }

    @Test
    void testNonEmptyDelimiterWithNullRegex() {
        when(mockConfig.getDelimiter()).thenReturn(",");
        when(mockConfig.getDelimiterRegex()).thenReturn(null);
        final SplitEventProcessor processor = new SplitEventProcessor(pluginMetrics, mockConfig);

        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "a,b");
        final Record<Event> record = createTestRecord(testData);

        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData().get("k1", Object.class), equalTo("a"));
        assertThat(results.get(1).getData().get("k1", Object.class), equalTo("b"));
    }
}
