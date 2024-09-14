/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.AppendField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.Field;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.IndirectField;
import org.opensearch.dataprepper.plugins.processor.dissect.Fields.NormalField;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

        Field field1 = new NormalField("field1");
        Field field2 = new NormalField("field2");
        field1.setValue("foo");
        field2.setValue("bar");

        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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

        Field field1 = new AppendField("field1");
        Field field2 = new AppendField("field2");
        field1.setValue("foo");
        field2.setValue("bar");

        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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

        Field field1 = new IndirectField("field1");
        Field field2 = new IndirectField("field2");
        field1.setValue("foo");
        field2.setValue("bar");

        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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
    void test_dissectText_returns_false() throws NoSuchFieldException, IllegalAccessException {

        when(dissector.dissectText(any(String.class))).thenReturn(false);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        // assert event is not modified
        assertThat(dissectedRecords.get(0).getData(), is(record.getData()));
    }

    @Test
    void test_dissectText_throws_exception() throws NoSuchFieldException, IllegalAccessException {
        when(dissector.dissectText(any(String.class))).thenThrow(RuntimeException.class);

        final DissectProcessor processor = createObjectUnderTest();
        reflectivelySetDissectorMap(processor);
        final Record<Event> record = getEvent("");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        // assert event is not modified
        assertThat(dissectedRecords.get(0).getData(), is(record.getData()));
    }

    @Test
    void test_target_type_int() throws NoSuchFieldException, IllegalAccessException {

        Field field1 = new IndirectField("field1");
        Field field2 = new IndirectField("field2");
        field1.setValue("20");
        field2.setValue("30");
        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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
        Field field1 = new IndirectField("field1");
        Field field2 = new IndirectField("field2");
        field1.setValue("true");
        field2.setValue("30");
        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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
        Field field1 = new IndirectField("field1");
        Field field2 = new IndirectField("field2");
        field1.setValue("20.0");
        field2.setValue("30");
        when(dissector.dissectText(any(String.class))).thenReturn(true);
        when(dissector.getDissectedFields()).thenReturn(List.of(field1, field2));

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
}