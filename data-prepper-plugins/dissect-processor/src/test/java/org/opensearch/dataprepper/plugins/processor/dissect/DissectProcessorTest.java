/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

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
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DissectProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private DissectProcessorConfig dissectConfig;

    @Mock
    private Dissector dissector;

    @BeforeEach
    void setUp() {
        when(dissectConfig.getMap()).thenReturn(Map.of());
    }

    @Test
    void invalid_dissect_when_condition_throws_InvalidPluginConfigurationException() {
        final String dissectWhen = UUID.randomUUID().toString();

        when(dissectConfig.getDissectWhen()).thenReturn(dissectWhen);

        when(expressionEvaluator.isValidExpressionStatement(dissectWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void test_normal_fields_dissect_succeeded() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "foo", "field2", "bar"));

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_append_fields_dissect_succeeded() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "foo", "field2", "bar"));

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_indirect_fields_dissect_succeeded() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "foo", "field2", "bar"));

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_dissectText_returns_null_on_failure() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(null);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(dissectedRecords.get(0).getData(), is(record.getData()));
    }

    @Test
    void test_dissectText_throws_exception() throws NoSuchFieldException, IllegalAccessException {
        when(dissector.dissectText(any(String.class))).thenThrow(RuntimeException.class);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(dissectedRecords.get(0).getData(), is(record.getData()));
    }

    @Test
    void test_target_type_int() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "20", "field2", "30"));

        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.INTEGER);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Integer);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(20));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));
        assertTrue(dissectedRecords.get(0).getData().get("field2", Object.class) instanceof String);
        assertThat(dissectedRecords.get(0).getData().get("field2", Object.class), is("30"));
    }

    @Test
    void test_target_type_bool() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "true", "field2", "30"));

        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.BOOLEAN);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Boolean);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(true));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));
        assertTrue(dissectedRecords.get(0).getData().get("field2", Object.class) instanceof String);
        assertThat(dissectedRecords.get(0).getData().get("field2", Object.class), is("30"));
    }

    @Test
    void test_target_type_double() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "20.0", "field2", "30"));

        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.DOUBLE);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Double);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(20.0d));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));
        assertTrue(dissectedRecords.get(0).getData().get("field2", Object.class) instanceof String);
        assertThat(dissectedRecords.get(0).getData().get("field2", Object.class), is("30"));
    }

    private DissectProcessor createObjectUnderTest() {
        return new DissectProcessor(pluginMetrics, dissectConfig, expressionEvaluator);
    }

    private Record<Event> getEvent(String dissectText) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("test", dissectText);
        return buildRecordWithEvent(testData);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent
                                    .builder()
                                    .withData(data)
                                    .withEventType("event")
                                    .build());
    }

    private void reflectivelySetDissectorMap(DissectProcessor processor) throws NoSuchFieldException, IllegalAccessException {
        Map<String, Dissector> dissectorMap = Map.of("test", dissector);
        java.lang.reflect.Field reflectField = DissectProcessor.class.getDeclaredField("dissectorMap");

        try {
            reflectField.setAccessible(true);
            reflectField.set(processor, dissectorMap);
        } finally {
            reflectField.setAccessible(false);
        }
    }

    @Test
    void test_delete_source_requested() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("level", "WARN"));
        when(dissectConfig.isDeleteSourceRequested()).thenReturn(true);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> dataPrepperRecord = getEvent("2025-01-28T00:00:00.000Z WARN This is a test log");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(dataPrepperRecord));

        assertTrue(dissectedRecords.get(0).getData().containsKey("level"));
        assertThat(dissectedRecords.get(0).getData().get("level", String.class), is("WARN"));
        assertFalse(dissectedRecords.get(0).getData().containsKey("test"));
    }

    @Test
    void test_delete_source_not_requested() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("level", "WARN"));
        when(dissectConfig.isDeleteSourceRequested()).thenReturn(false);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> dataPrepperRecord = getEvent("2025-01-28T00:00:00.000Z WARN This is a test log");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(dataPrepperRecord));

        assertTrue(dissectedRecords.get(0).getData().containsKey("level"));
        assertThat(dissectedRecords.get(0).getData().get("level", String.class), is("WARN"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("test"));
    }

    @Test
    void test_delete_source_requested_dissect_fail() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(null);
        when(dissectConfig.isDeleteSourceRequested()).thenReturn(true);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> dataPrepperRecord = getEvent("2025-01-28T00:00:00.000Z WARN This is a test log");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(dataPrepperRecord));

        assertTrue(dissectedRecords.get(0).getData().containsKey("test"));
    }

    @Test
    void test_dissect_when_condition_false_skips_event() throws NoSuchFieldException, IllegalAccessException {
        final String dissectWhen = UUID.randomUUID().toString();
        when(dissectConfig.getDissectWhen()).thenReturn(dissectWhen);
        when(expressionEvaluator.isValidExpressionStatement(dissectWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(dissectWhen), any())).thenReturn(false);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("some text");
        processor.doExecute(Collections.singletonList(record));

        org.mockito.Mockito.verify(dissector, org.mockito.Mockito.never()).dissectText(any());
    }

    @Test
    void test_dissect_when_condition_true_processes_event() throws NoSuchFieldException, IllegalAccessException {
        final String dissectWhen = UUID.randomUUID().toString();
        when(dissectConfig.getDissectWhen()).thenReturn(dissectWhen);
        when(expressionEvaluator.isValidExpressionStatement(dissectWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(dissectWhen), any())).thenReturn(true);
        when(dissector.dissectText(any(String.class))).thenReturn(Map.of("field1", "foo"));

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("some text");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        org.mockito.Mockito.verify(dissector).dissectText(any());
        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
    }

    @Test
    void test_concurrent_doExecute_no_cross_contamination() throws InterruptedException {
        when(dissectConfig.getMap()).thenReturn(Map.of("message", "%{timestamp} %{level} %{content}"));
        final DissectProcessor processor = createObjectUnderTest();

        int threadCount = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<String> errors = Collections.synchronizedList(new ArrayList<>());

        for (int t = 0; t < threadCount; t++) {
            final String timestamp = "2024-01-" + String.format("%02d", t + 1);
            final String level = "LEVEL" + t;
            final String content = "content" + t;
            final String input = timestamp + " " + level + " " + content;

            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        final Map<String, Object> data = new HashMap<>();
                        data.put("message", input);
                        final Record<Event> record = new Record<>(JacksonEvent.builder()
                                .withData(data).withEventType("event").build());
                        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
                        final Event resultEvent = results.get(0).getData();
                        if (!timestamp.equals(resultEvent.get("timestamp", String.class))) {
                            errors.add("timestamp mismatch: expected " + timestamp + " got " + resultEvent.get("timestamp", String.class));
                        }
                        if (!level.equals(resultEvent.get("level", String.class))) {
                            errors.add("level mismatch: expected " + level + " got " + resultEvent.get("level", String.class));
                        }
                        if (!content.equals(resultEvent.get("content", String.class))) {
                            errors.add("content mismatch: expected " + content + " got " + resultEvent.get("content", String.class));
                        }
                    }
                } catch (Exception e) {
                    errors.add(e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS), "Test timed out");
        executor.shutdown();

        assertTrue(errors.isEmpty(), "Concurrency errors: " + errors);
    }

}
