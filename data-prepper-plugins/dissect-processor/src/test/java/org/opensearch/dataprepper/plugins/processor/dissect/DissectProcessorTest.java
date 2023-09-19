/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.dissect;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DissectProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private DissectProcessorConfig dissectConfig;


    @Test
    void test_normal_field_trailing_spaces(){
        Map<String, String> dissectMap = Map.of("test", " %{field1} %{field2} ");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(" foo bar ");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));

    }

    @Test
    void test_normal_field_without_trailing_spaces(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{field1} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));

    }

    @Test
    void test_normal_field_failure_without_delimiters(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{field1} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(dissectedRecords.get(0).getData().containsKey("field1"));
        assertFalse(dissectedRecords.get(0).getData().containsKey("field2"));
    }

    @Test
    void test_normal_field_failure_with_extra_whitespaces(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{field1} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(" dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(dissectedRecords.get(0).getData().containsKey("field1"));
        assertFalse(dissectedRecords.get(0).getData().containsKey("field2"));
    }

    @Test
    void test_named_skip_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{?field1} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_unnamed_skip_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));

        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_indirect_field_with_skip_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{?field1} %{&field1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("foo"));

        assertThat(dissectedRecords.get(0).getData().get("foo", String.class), is("bar"));
    }

    @Test
    void test_indirect_field_with_normal_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{field1} %{&field1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().containsKey("foo"));
        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
        assertThat(dissectedRecords.get(0).getData().get("foo", String.class), is("bar"));
    }


    @Test
    void test_append_field_without_index(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{+field1} %{+field1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foobar"));
    }

    @Test
    void test_append_field_with_index(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{+field1/2} %{+field1/1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("barfoo"));
    }

    @Test
    void test_append_whitespace_normal_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{field1->} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo      bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foo"));
    }

    @Test
    void test_append_whitespace_append_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{+field1->} %{+field1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo      bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertThat(dissectedRecords.get(0).getData().get("field1", String.class), is("foobar"));
    }

    @Test
    void test_append_whitespace_indirect_field(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{?field1->} %{&field1} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo      bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("foo"));
        assertThat(dissectedRecords.get(0).getData().get("foo", String.class), is("bar"));
    }

    @Test
    void test_skip_fields(){
        Map<String, String> dissectMap = Map.of("test", "dm1 %{?field1->} %{?field3} %{field2} dm2");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("dm1 foo     skip   bar dm2");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field2"));
        assertThat(dissectedRecords.get(0).getData().get("field2", String.class), is("bar"));
    }

    @Test
    void test_normal_fields(){
        Map<String, String> dissectMap = Map.of("test", "%{id->} %{function} %{server}");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("00000043     ViewReceive machine-321");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("function"));
        assertThat(dissectedRecords.get(0).getData().get("function", String.class), is("ViewReceive"));
    }

    @Test
    void test_indirect_field_with_append(){
        Map<String, String> dissectMap = Map.of("test", "%{+field1->} %{+field1} %{&field1->}");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("foo     bar result     ");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("foobar"));
        assertThat(dissectedRecords.get(0).getData().get("foobar", String.class), is("result"));
    }

    @Test
    void test_target_type_int(){
        Map<String, String> dissectMap = Map.of("test", "%{field1} %{field2}");
        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.INTEGER);
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("20 30");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Integer);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(20));
    }

    @Test
    void test_target_type_default(){
        Map<String, String> dissectMap = Map.of("test", "%{field1} %{field2}");
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("20 30");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof String);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is("20"));
    }

    @Test
    void test_target_type_bool(){
        Map<String, String> dissectMap = Map.of("test", "%{field1} %{field2}");
        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.BOOLEAN);
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("true 30");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Boolean);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(true));
    }

    @Test
    void test_target_type_double(){
        Map<String, String> dissectMap = Map.of("test", "%{field1} %{field2}");
        Map<String, TargetType> targetsMap = Map.of("field1", TargetType.DOUBLE);
        when(dissectConfig.getMap()).thenReturn(dissectMap);
        when(dissectConfig.getTargetTypes()).thenReturn(targetsMap);
        final DissectProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("20.5 30");
        final List<Record<Event>> dissectedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(dissectedRecords.get(0).getData().containsKey("field1"));
        assertTrue(dissectedRecords.get(0).getData().get("field1", Object.class) instanceof Double);
        assertThat(dissectedRecords.get(0).getData().get("field1", Object.class), is(20.5));
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

}