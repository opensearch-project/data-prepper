/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.common.TransformOption;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KeyValueProcessorTests {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessorTests.class);

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private KeyValueProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    void setup() {
        final KeyValueProcessorConfig defaultConfig = new KeyValueProcessorConfig();
        lenient().when(mockConfig.getNormalizeKeys()).thenReturn(false);
        lenient().when(mockConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(mockConfig.getStringLiteralCharacter()).thenReturn(null);
        lenient().when(mockConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        lenient().when(mockConfig.getFieldDelimiterRegex()).thenReturn(defaultConfig.getFieldDelimiterRegex());
        lenient().when(mockConfig.getFieldSplitCharacters()).thenReturn(defaultConfig.getFieldSplitCharacters());
        lenient().when(mockConfig.getIncludeKeys()).thenReturn(defaultConfig.getIncludeKeys());
        lenient().when(mockConfig.getExcludeKeys()).thenReturn(defaultConfig.getExcludeKeys());
        lenient().when(mockConfig.getDefaultValues()).thenReturn(defaultConfig.getDefaultValues());
        lenient().when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(defaultConfig.getKeyValueDelimiterRegex());
        lenient().when(mockConfig.getValueSplitCharacters()).thenReturn(defaultConfig.getValueSplitCharacters());
        lenient().when(mockConfig.getNonMatchValue()).thenReturn(defaultConfig.getNonMatchValue());
        lenient().when(mockConfig.getPrefix()).thenReturn(defaultConfig.getPrefix());
        lenient().when(mockConfig.getDeleteKeyRegex()).thenReturn(defaultConfig.getDeleteKeyRegex());
        lenient().when(mockConfig.getDeleteValueRegex()).thenReturn(defaultConfig.getDeleteValueRegex());
        lenient().when(mockConfig.getTransformKey()).thenReturn(defaultConfig.getTransformKey());
        lenient().when(mockConfig.getWhitespace()).thenReturn(defaultConfig.getWhitespace());
        lenient().when(mockConfig.getSkipDuplicateValues()).thenReturn(defaultConfig.getSkipDuplicateValues());
        lenient().when(mockConfig.getRemoveBrackets()).thenReturn(defaultConfig.getRemoveBrackets());
        lenient().when(mockConfig.getRecursive()).thenReturn(defaultConfig.getRecursive());
        lenient().when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(defaultConfig.getOverwriteIfDestinationExists());
        lenient().when(mockConfig.getValueGrouping()).thenReturn(false);
        lenient().when(mockConfig.getDropKeysWithNoValue()).thenReturn(false);

        final String keyValueWhen = UUID.randomUUID().toString();
        lenient().when(mockConfig.getKeyValueWhen()).thenReturn(keyValueWhen);
        lenient().when(expressionEvaluator.isValidExpressionStatement(keyValueWhen)).thenReturn(true);
        lenient().when(expressionEvaluator.evaluateConditional(eq(keyValueWhen), any(Event.class))).thenReturn(true);
    }

    private KeyValueProcessor createObjectUnderTest() {
        return new KeyValueProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    @Test
    void invalid_expression_statement_throws_InvalidPluginConfigurationException() {
        final String keyValueWhen = UUID.randomUUID().toString();

        when(mockConfig.getKeyValueWhen()).thenReturn(keyValueWhen);

        when(expressionEvaluator.isValidExpressionStatement(keyValueWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void testSingleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void do_not_modify_event_when_the_expression_evaluation_returns_false() {
        final String keyValueWhen = UUID.randomUUID().toString();
        when(mockConfig.getKeyValueWhen()).thenReturn(keyValueWhen);
        when(expressionEvaluator.isValidExpressionStatement(keyValueWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(keyValueWhen), any(Event.class))).thenReturn(false);


        final KeyValueProcessor objectUnderTest = createObjectUnderTest();

        final Record<Event> record = getMessage("key1=value1");
        final Map<String, Object> eventMap = record.getData().toMap();

        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData(), notNullValue());
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(eventMap));
    }

    @Test
    void testKeyValueProcessorWithoutMessage() {
        final Map<String, Object> testData = new HashMap();
        testData.put("notMessage", "not a message");
        final Record<Event> record = buildRecordWithEvent(testData);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        LinkedHashMap<String, Object> parsed_message = editedRecords.get(0).getData().get("parsed_message", LinkedHashMap.class);
        assertThat(parsed_message, equalTo(null));
    }

    @Test
    void testMultipleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDropKeysWithNoValue() {
        lenient().when(mockConfig.getDropKeysWithNoValue()).thenReturn(true);

        final Record<Event> record = getMessage("key1=value1&key2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @ParameterizedTest
    @MethodSource("getKeyValueGroupingTestdata")
    void testMultipleKvToObjectKeyValueProcessorWithValueGrouping(String fieldDelimiters, String input, Map<String, Object> expectedResultMap) {
        lenient().when(mockConfig.getValueGrouping()).thenReturn(true);
        lenient().when(mockConfig.getStringLiteralCharacter()).thenReturn('\"');
        lenient().when(mockConfig.getDropKeysWithNoValue()).thenReturn(true);
        lenient().when(mockConfig.getFieldSplitCharacters()).thenReturn(fieldDelimiters);
        final KeyValueProcessor objectUnderTest = createObjectUnderTest();
        final Record<Event> record = getMessage(input);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(expectedResultMap.size()));
        for (Map.Entry<String, Object>entry: expectedResultMap.entrySet()) {
            assertThatKeyEquals(parsed_message, entry.getKey(), entry.getValue());
        }
    }

    private static Stream<Arguments> getKeyValueGroupingTestdata() {
        return Stream.of (
                Arguments.of(", ", "key1=value1,key2=value2", Map.of("key1", "value1", "key2", "value2")),
                Arguments.of(", ", "key1=value1 key2=value2", Map.of("key1", "value1", "key2", "value2")),
                Arguments.of(", ", "key1=value1 ,key2=value2", Map.of("key1", "value1", "key2", "value2")),
                Arguments.of(", ", "key1=value1, key2=value2", Map.of("key1", "value1", "key2", "value2")),
                Arguments.of(", ", "key1=It\\'sValue1, key2=value2", Map.of("key1", "It\\'sValue1", "key2", "value2")),
                Arguments.of(", ", "text1 text2 key1=value1, key2=value2 text3 text4", Map.of("key1", "value1", "key2", "value2")),
                Arguments.of(", ", "text1 text2 foo key1=value1 url=http://foo.com?bar=text,text&foo=zoo bar k2=\"http://bar.com?a=b&c=foo bar\" barr", Map.of("key1", "value1", "url", "http://foo.com?bar=text,text&foo=zoo", "k2", "\"http://bar.com?a=b&c=foo bar\"")),
                Arguments.of(", ", "vendorMessage=VendorMessage(uid=1847060493-1712778523223, feedValue=https://syosetu.org/novel/147705/15.html, bundleId=, linkType=URL, vendor=DOUBLEVERIFY, platform=DESKTOP, deviceTypeId=1, bidCount=6, appStoreTld=, feedSource=DSP, regions=[APAC], timestamp=1712778523223, externalId=)", Map.of("vendorMessage", "VendorMessage(uid=1847060493-1712778523223, feedValue=https://syosetu.org/novel/147705/15.html, bundleId=, linkType=URL, vendor=DOUBLEVERIFY, platform=DESKTOP, deviceTypeId=1, bidCount=6, appStoreTld=, feedSource=DSP, regions=[APAC], timestamp=1712778523223, externalId=)")),
                Arguments.of(", ()", "foo bar(key1=value1, key2=value2, key3=)", Map.of("key1", "value1", "key2", "value2", "key3","")),
                Arguments.of(", ", "foo bar(key1=value1, key2=value2, key3=)", Map.of("bar(key1", "value1", "key2", "value2", "key3",")")),
                Arguments.of(", ", "foo bar[key1=value1, key2=value2, key3=]", Map.of("bar[key1", "value1", "key2", "value2", "key3","]")),
                Arguments.of(", ", "foo bar{key1=value1, key2=value2, key3=}", Map.of("bar{key1", "value1", "key2", "value2", "key3","}")),
                Arguments.of(", ", "key1 \"key2=val2\" key3=\"value3,value4\"", Map.of("key3", "\"value3,value4\"")),
                Arguments.of(", ", "key1=[value1,value2], key3=value3", Map.of("key1", "[value1,value2]", "key3", "value3")),
                Arguments.of(", ", "key1=(value1, value2), key3=value3", Map.of("key1", "(value1, value2)", "key3", "value3")),
                Arguments.of(", ", "key1=<value1 ,value2>, key3=value3", Map.of("key1", "<value1 ,value2>", "key3", "value3")),
                Arguments.of(", ", "key1={value1,value2}, key3=value3", Map.of("key1", "{value1,value2}", "key3", "value3")),
                Arguments.of(", ", "key1='value1,value2', key3=value3", Map.of("key1", "'value1,value2'", "key3", "value3")),
                Arguments.of(", ", "foo  key1=val1, key2=val2,key3=val3 bar", Map.of("key1", "val1", "key2", "val2", "key3", "val3")),
                Arguments.of(", ", "foo,key1=(val1,key2=val2,val3),key4=val4 bar", Map.of("key1", "(val1,key2=val2,val3)", "key4", "val4")),
                Arguments.of(", ", "foo,key1=(val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "(val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo,key1=[val1,key2=val2,val3],key4=val4 bar", Map.of("key1", "[val1,key2=val2,val3]", "key4", "val4")),
                Arguments.of(", ", "foo,key1=[val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "[val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo,key1={val1,key2=val2,val3},key4=val4 bar", Map.of("key1", "{val1,key2=val2,val3}", "key4", "val4")),
                Arguments.of(", ", "foo,key1={val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "{val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo,key1=<val1,key2=val2,val3>,key4=val4 bar", Map.of("key1", "<val1,key2=val2,val3>", "key4", "val4")),
                Arguments.of(", ", "foo,key1=<val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "<val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo,key1=\"val1,key2=val2,val3\",key4=val4 bar", Map.of("key1", "\"val1,key2=val2,val3\"", "key4", "val4")),
                Arguments.of(", ", "foo,key1=\"val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "\"val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo,key1='val1,key2=val2,val3',key4=val4 bar", Map.of("key1", "'val1,key2=val2,val3'", "key4", "val4")),
                Arguments.of(", ", "foo,key1='val1,key2=val2,val3,key4=val4 bar", Map.of("key1", "'val1,key2=val2,val3,key4=val4 bar")),

                Arguments.of(", ", "foo \"key1=key2 bar\" key2=val2 baz", Map.of("key2", "val2")),
                Arguments.of(", ", "foo  key1=https://bar.baz/?key2=val2&url=https://quz.fred/ bar", Map.of("key1","https://bar.baz/?key2=val2&url=https://quz.fred/")),
                Arguments.of(", ", "foo key1=\"bar \" qux\" fred", Map.of("key1", "\"bar \"")),
                Arguments.of(", ", "foo key1=\"bar \\\" qux\" fred", Map.of("key1", "\"bar \\\" qux\"")),

                Arguments.of(", ", "key1=\"value1,value2\", key3=value3", Map.of("key1", "\"value1,value2\"", "key3", "value3"))
               );
    }

    @Test
    void testValueGroupingWithOutStringLiterals() {
        when(mockConfig.getDestination()).thenReturn(null);
        String message = "text1 text2 [ key1=value1  value2";
        lenient().when(mockConfig.getStringLiteralCharacter()).thenReturn(null);
        lenient().when(mockConfig.getFieldSplitCharacters()).thenReturn(" ,");
        lenient().when(mockConfig.getValueGrouping()).thenReturn(true);
        final Record<Event> record = getMessage(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();
        assertThat(event.containsKey("parsed_message"), is(false));

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"", "'"})
    void testStringLiteralCharacter(String literalString) {
        when(mockConfig.getDestination()).thenReturn(null);
        String message = literalString+"ignore this "+literalString+" key1=value1&key2=value2 "+literalString+"ignore=this&too"+literalString;
        lenient().when(mockConfig.getStringLiteralCharacter()).thenReturn(literalString.charAt(0));
        lenient().when(mockConfig.getFieldSplitCharacters()).thenReturn(" &");
        lenient().when(mockConfig.getValueGrouping()).thenReturn(true);
        final Record<Event> record = getMessage(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();
        assertThat(event.containsKey("parsed_message"), is(false));

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
    }

    @Test
    void testWriteToRoot() {
        when(mockConfig.getDestination()).thenReturn(null);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();
        assertThat(event.containsKey("parsed_message"), is(false));

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testInvalidKeyCharsReplacement() {
        when(mockConfig.getDestination()).thenReturn(null);
        when(mockConfig.getNormalizeKeys()).thenReturn(true);
        final Record<Event> record = getMessage("key%1=value1&key^2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("key_1"), is(true));
        assertThat(event.containsKey("key_2"), is(true));
        assertThat(event.get("key_1", Object.class), is("value1"));
        assertThat(event.get("key_2", Object.class), is("value2"));
    }

    @Test
    void testWriteToRootWithOverwrite() {
        when(mockConfig.getDestination()).thenReturn(null);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("key1", "value to be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testWriteToDestinationWithOverwrite() {
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("parsed_message", "value to be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testWriteToRootWithOverwriteDisabled() {
        when(mockConfig.getDestination()).thenReturn(null);
        when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("key1", "value will not be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value will not be overwritten"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testWriteToDestinationWithOverwriteDisabled() {
        when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("parsed_message", "value will not be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("parsed_message"), is(true));
        assertThat(event.get("parsed_message", Object.class), is("value will not be overwritten"));
    }

    @Test
    void testSingleRegexFieldDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":_*:");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);

        final Record<Event> record = getMessage("key1=value1:_____:key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        LOG.info("parsedMessage={}", parsed_message);
        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testBothKeyValuesDefinedErrorKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(":\\+*:");

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void testBothFieldsDefinedErrorKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":\\+*:");

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void testSingleRegexKvDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(":\\+*:");
        when(mockConfig.getValueSplitCharacters()).thenReturn(null);

        final Record<Event> record = getMessage("key1:++:value1&key2:+:value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testBadKeyValueDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn("[");
        when(mockConfig.getValueSplitCharacters()).thenReturn(null);

        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, this::createObjectUnderTest);
        assertThat(e.getMessage(), CoreMatchers.startsWith("key_value_delimiter"));
    }

    @Test
    void testBadFieldDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn("[");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);

        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, this::createObjectUnderTest);
        assertThat(e.getMessage(), CoreMatchers.startsWith("field_delimiter"));
    }

    @Test
    void testBadDeleteKeyRegexKeyValueProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("[");
        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, this::createObjectUnderTest);
        assertThat(e.getMessage(), CoreMatchers.startsWith("delete_key_regex"));
    }

    @Test
    void testBadDeleteValueRegexKeyValueProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("[");
        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, this::createObjectUnderTest);
        assertThat(e.getMessage(), CoreMatchers.startsWith("delete_value_regex"));
    }

    @Test
    void testDuplicateKeyToArrayValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value2&key1=value3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList<>();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add("value3");
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testDuplicateKeyToArrayWithNonMatchValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value2&key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testFieldSplitCharactersKeyValueProcessor() {
        when(mockConfig.getFieldSplitCharacters()).thenReturn("&!");

        final Record<Event> record = getMessage("key1=value1&key1=value2!key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testFieldSplitCharactersDoesntSupercedeDelimiterKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":d+:");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);

        final Record<Event> record = getMessage("key1=value1:d:key1=value2:d:key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testIncludeKeysKeyValueProcessor() {
        final List<String> includeKeys = List.of("key2", "key3");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);

        final Record<Event> record = getMessage("key1=value1&key2=value2&key3=value3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key2", "value2");
        assertThatKeyEquals(parsed_message, "key3", "value3");
    }

    @Test
    void testIncludeKeysNoMatchKeyValueProcessor() {
        final List<String> includeKeys = Collections.singletonList("noMatch");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
    }

    @Test
    void testIncludeKeysAsDefaultKeyValueProcessor() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of());

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testExcludeKeysKeyValueProcessor() {
        final List<String> excludeKeys = List.of("key2");
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testExcludeKeysAsDefaultKeyValueProcessor() {
        when(mockConfig.getExcludeKeys()).thenReturn(List.of());

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testIncludeExcludeKeysOverlapKeyValueProcessor() {
        final List<String> includeKeys = List.of("key1", "key3");
        final List<String> excludeKeys = List.of("key3");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void testDefaultKeysNoOverlapsBetweenEventKvProcessor() {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "dKey", "dValue");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultKeysAlreadyInMessageKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);

        final Record<Event> record = getMessage("key1=value1&dKey=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "dKey", "abc");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultIncludeKeysOverlapKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key1", "abc");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultPrioritizeIncludeKeysKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key2", "value2");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);

        final Record<Event> record = getMessage("key1=value1&key2=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIncludeKeysNotInRecordMessageKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key2", "value2");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);

        final Record<Event> record = getMessage("key2=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDefaultExcludeKeysOverlapKeyValueProcessor() {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        final List<String> excludeKeys = List.of("dKey");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void testCustomPrefixKvProcessor() {
        when(mockConfig.getPrefix()).thenReturn("TEST_");

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "TEST_key1", "value1");
    }

    @Test
    void testDefaultNonMatchValueKvProcessor() {
        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", null);
    }

    @Test
    void testCustomStringNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn("BAD_MATCH");

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", "BAD_MATCH");
    }

    @Test
    void testCustomBoolNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn(true);

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", true);
    }

    @Test
    void testDeleteKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testDeleteValueRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1=value1   &key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueWithNonStringRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");
        when(mockConfig.getNonMatchValue()).thenReturn(3);

        final Record<Event> record = getMessage("key1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", 3);
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueAndKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1  &  key2 = value2 ");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testLowercaseTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn(TransformOption.LOWERCASE);

        final Record<Event> record = getMessage("Key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testUppercaseTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn(TransformOption.UPPERCASE);

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "KEY1", "value1");
    }

    @Test
    void testCapitalizeTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn(TransformOption.CAPITALIZE);

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsedMessage = getLinkedHashMap(editedRecords);

        assertThat(parsedMessage.size(), equalTo(1));
        assertThatKeyEquals(parsedMessage, "Key1", "value1");
    }

    @Test
    void testStrictWhitespaceKvProcessor() {
        when(mockConfig.getWhitespace()).thenReturn(WhitespaceOption.STRICT);

        final Record<Event> record = getMessage("key1  =  value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testFalseSkipDuplicateValuesKvProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value1");

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testTrueSkipDuplicateValuesKvProcessor() {
        when(mockConfig.getSkipDuplicateValues()).thenReturn(true);

        final Record<Event> record = getMessage("key1=value1&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testTrueThreeInputsDuplicateValuesKvProcessor() {
        when(mockConfig.getSkipDuplicateValues()).thenReturn(true);

        final Record<Event> record = getMessage("key1=value1&key1=value2&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testTrueRemoveBracketsKvProcessor() {
        when(mockConfig.getRemoveBrackets()).thenReturn(true);

        final Record<Event> record = getMessage("key1=(value1)&key2=[value2]&key3=<value3>");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(3));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
        assertThatKeyEquals(parsed_message, "key3", "value3");
    }

    @Test
    void testTrueRemoveMultipleBracketsKvProcessor() {
        when(mockConfig.getRemoveBrackets()).thenReturn(true);

        final Record<Event> record = getMessage("key1=((value1)&key2=[value1][value2]");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value1value2");
    }

    @Test
    void testBasicRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testMultiRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);

        final Record<Event> record = getMessage("item1=[item1-subitem1=(inner1=abc&inner2=xyz)&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        final Map<String, Object> nestedInnerMap = new HashMap<>();

        nestedInnerMap.put("inner1", "abc");
        nestedInnerMap.put("inner2", "xyz");
        expectedValueMap.put("item1-subitem1", nestedInnerMap);
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testTransformKeyRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getTransformKey()).thenReturn(TransformOption.CAPITALIZE);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "Item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "Item2", "item2-value");
    }

    @Test
    void testIncludeInnerKeyRecursiveKvProcessor() {
        final List<String> includeKeys = List.of("item1-subitem1");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
    }

    @Test
    void testExcludeInnerKeyRecursiveKvProcessor() {
        final List<String> excludeKeys = List.of("item1-subitem1");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testDefaultInnerKeyRecursiveKvProcessor() {
        final Map<String, Object> defaultMap = Map.of("item1-subitem1", "default");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(3));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
        assertThatKeyEquals(parsed_message, "item1-subitem1", "default");
    }

    @Test
    void testTagsAddedWhenParsingFails() {
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getTagsOnFailure()).thenReturn(List.of("tag1", "tag2"));

        final Record<Event> record = getMessage("item1=[]");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) createObjectUnderTest().doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
        assertThat(record.getData().getMetadata().hasTags(List.of("tag1", "tag2")), is(true));
    }

    @Test
    void testShutdownIsReady() {
        assertThat(createObjectUnderTest().isReadyForShutdown(), is(true));
    }
        
    @Test
    void testKeyValueProcessorWithRe2j() {
        System.setProperty("dataprepper.pattern.provider", "re2j");
        try {
            when(mockConfig.getFieldDelimiterRegex()).thenReturn("&");
            when(mockConfig.getKeyValueDelimiterRegex()).thenReturn("=");
            KeyValueProcessor processor = createObjectUnderTest();
            
            final Record<Event> record = getMessage("key1=value1&key2=value2");
            final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
            
            assertThat(editedRecords.size(), equalTo(1));
        } finally {
            System.clearProperty("dataprepper.pattern.provider");
        }
    }
    
    private Record<Event> getMessage(String message) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    private LinkedHashMap<String, Object> getLinkedHashMap(List<Record<Event>> editedRecords) {
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        LinkedHashMap<String, Object> parsed_message = editedRecords.get(0).getData().get("parsed_message", LinkedHashMap.class);
        assertThat(parsed_message, notNullValue());
        return parsed_message;
    }

    private void assertThatKeyEquals(final LinkedHashMap<String, Object> parsed_message, final String key, final Object value) {
        assertThat(parsed_message.containsKey(key), is(true));
        assertThat(parsed_message.get(key), equalTo(value));
    }
}
