/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.obfuscation.action.ObfuscationAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ObfuscationProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory mockFactory;

    @Mock
    private ObfuscationProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private ObfuscationProcessor obfuscationProcessor;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Record<Event> createRecord(String message) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    @BeforeEach
    void setup() {
        final ObfuscationProcessorConfig defaultConfig = new ObfuscationProcessorConfig("message", null, null, null, null, false);
        lenient().when(mockConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(mockConfig.getAction()).thenReturn(defaultConfig.getAction());
        lenient().when(mockConfig.getPatterns()).thenReturn(defaultConfig.getPatterns());
        lenient().when(mockConfig.getTarget()).thenReturn(defaultConfig.getTarget());
        lenient().when(mockConfig.getObfuscateWhen()).thenReturn(null);
        lenient().when(mockConfig.getTagsOnMatchFailure()).thenReturn(List.of(UUID.randomUUID().toString()));
        lenient().when(mockConfig.getSingleWordOnly()).thenReturn(defaultConfig.getSingleWordOnly());
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);
    }

    @Test
    void obfuscate_when_evaluates_to_false_does_not_modify_event() {
        final String expression = "/test == success";
        final Record<Event> record = createRecord(UUID.randomUUID().toString());
        when(mockConfig.getObfuscateWhen()).thenReturn(expression);
        when(expressionEvaluator.evaluateConditional(expression, record.getData())).thenReturn(false);

        final ObfuscationProcessor objectUnderTest = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Map<String, Object> expectedEventMap = record.getData().toMap();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(expectedEventMap));
    }

    @Test
    void event_is_tagged_with_match_failure_tags_when_it_does_not_match_any_patterns_and_when_condition_is_true() {
        final Record<Event> record = createRecord(UUID.randomUUID().toString());

        final String expression = UUID.randomUUID().toString();
        when(mockConfig.getObfuscateWhen()).thenReturn(expression);
        when(expressionEvaluator.evaluateConditional(expression, record.getData())).thenReturn(true);
        when(mockConfig.getPatterns()).thenReturn(List.of(UUID.randomUUID().toString()));

        final ObfuscationProcessor objectUnderTest = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Map<String, Object> expectedEventMap = record.getData().toMap();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(expectedEventMap));
        assertThat(editedRecords.get(0).getData().getMetadata().getTags(), notNullValue());
        assertThat(editedRecords.get(0).getData().getMetadata().getTags().size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().getMetadata().getTags().contains(mockConfig.getTagsOnMatchFailure().get(0)), equalTo(true));
    }


    @ParameterizedTest
    @ValueSource(strings = {"hello", "hello, world", "This is a message", "123", "你好"})
    void testBasicProcessor(String message) {
        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        String result = editedRecords.get(0).getData().get("message", String.class);
        assertThat(result, equalTo("***"));
    }

    @Test
    void testProcessorWithDifferentAction() {
        final PluginModel mockModel = mock(PluginModel.class);
        final ObfuscationAction mockAction = mock(ObfuscationAction.class);
        when(mockModel.getPluginName()).thenReturn("mock");
        when(mockModel.getPluginSettings()).thenReturn(new HashMap<>());
        when(mockConfig.getAction()).thenReturn(mockModel);
        when(mockConfig.getTarget()).thenReturn("");
        when(mockAction.obfuscate(anyString(), anyList())).thenReturn("abc");

        when(mockFactory.loadPlugin(eq(ObfuscationAction.class), any(PluginSetting.class)))
                .thenReturn(mockAction);
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord("Hello");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo("abc"));
    }


    @ParameterizedTest
    @ValueSource(strings = {"hello", "hello, world", "This is a message", "123", "你好"})
    void testProcessorWithTarget(String message) {
        when(mockConfig.getTarget()).thenReturn("new_message");
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();

        assertTrue(data.containsKey("message"));
        assertTrue(data.containsKey("new_message"));

        assertThat(data.get("message", String.class), equalTo(message));
        assertThat(data.get("new_message", String.class), equalTo("***"));
    }

    @Test
    void testProcessorWithUnknownSource() {
        when(mockConfig.getSource()).thenReturn("email");
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord("Hello");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo("Hello"));
    }


    @ParameterizedTest
    @CsvSource({
            "Hello,He,***llo",
            "Hello,Hello,***",
            "Hello 33,\\d+,Hello ***",
            "33 Hello,\\d+,*** Hello",
            "My email is abc@test.com and my name is abc.,%{EMAIL_ADDRESS},My email is *** and my name is abc.",
            "My name is abc and my email is abc@test.com.,%{EMAIL_ADDRESS},My name is abc and my email is ***.",
            "Two emails are abc@test.com and abc@example.com.,%{EMAIL_ADDRESS},Two emails are *** and ***.",
    })
    void testProcessorWithPattern(String message, String pattern, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of(pattern));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @Test
    void testProcessorWithUnknownPattern() {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{UNKNOWN}"));
        assertThrows(InvalidPluginConfigurationException.class, () -> new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator));
    }

    @Test
    void testProcessorInvalidPattern() {
        when(mockConfig.getPatterns()).thenReturn(List.of("["));
        assertThrows(InvalidPluginConfigurationException.class, () -> new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator));
    }

    @ParameterizedTest
    @CsvSource({
            "abc@test.com,***",
            "123@test.com,***",
            "abc123@test.com,***",
            "abc_123@test.com,***",
            "a-b@test.com,***",
            "a.b@test.com,***",
            "abc@test-test.com,***",
            "abc@test.com.cn,***",
            "abc@test.mail.com.org,***",
    })
    void testProcessorWithEmailAddressPattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{EMAIL_ADDRESS}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "1555 555 5555,***",
            "5555555555,***",
            "1-555-555-5555,***",
            "1-(555)-555-5555,***",
            "1(555) 555 5555,***",
            "(555) 555 5555,***",
            "+1-555-555-5555,***",
            "My phone number is 1555 555 5555.,My phone number is ***.",
            "1555 555 5555 is my phone number,*** is my phone number",
            "15555,15555",
            "(020) 3333 4444,(020) 3333 4444"
    })
    void testProcessorWithUSPhoneNumberPattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{US_PHONE_NUMBER}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();

        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "5555555555554444,***",
            "4111111111111111,***",
            "1234567890123456,***",
            "1234 5678 9012 3456,***",
            "1234-5678-9012-3456,***",
            "My credit card is 1234-5678-9012-3456.,My credit card is ***.",
            "1234-5678-9012-3456 is my credit card number,*** is my credit card number",
            "123456789012,123456789012",
            "55555555555544400000,***0000"
    })
    void testProcessorWithCreditNumberPattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{CREDIT_CARD_NUMBER}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "1.1.1.1,***",
            "192.168.1.1,***",
            "255.255.255.0,***",
            "255.255.25.25,***",
            "256.256.25.25,256.256.25.25",
            "1.1.1.,1.1.1.",
            "My ip is 1.1.1.1.,My ip is ***.",
            "1.1.1.1 is my ip,*** is my ip"
    })
    void testProcessorWithIPAddressV4Pattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{IP_ADDRESS_V4}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }


    @ParameterizedTest
    @CsvSource({
            "000-00-0000,***",
            "123-11-1234,***",
            "123 11 1234,123 11 1234",
            "11-11-2011,11-11-2011",
    })
    void testProcessorWithUSSSNPattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{US_SSN_NUMBER}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "1.1,***",
            ".1,***",
            "1.,***.",
            "2022-01-01,***-***-***",
            "255.255.25.25,*********",
            "$3.5,$***",
            "+1.1,+***",
            "-1.1,-***",
    })
    void testProcessorWithBaseNumberPattern(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{BASE_NUMBER}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }
    
    @ParameterizedTest
    @CsvSource({
            "My email is abc@test.com,My email is ***",
            "My IP is 1.1.1.1 and my phone is 1-555-555-5555,My IP is *** and my phone is 1-555-555-5555",
            "My email is abc@test.com and my IP is 1.1.1.1,My email is *** and my IP is ***",
            "Hello World,Hello World",
    })
    void testProcessorWithMultiplePatterns(String message, String expected) {
        when(mockConfig.getPatterns()).thenReturn(List.of("%{EMAIL_ADDRESS}", "%{IP_ADDRESS_V4}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
            "My email is abc@test.com,%{EMAIL_ADDRESS},My email is ***",
            "testing this functionality, test, testing this functionality",
            "test this functionality, test, *** this functionality",
            "My IP is 1.1.1.1,%{IP_ADDRESS_V4},My IP is ***",
            "fd55555069-e7a9-11ee4111111111111111,%{CREDIT_CARD_NUMBER},fd55555069-e7a9-11ee4111111111111111",
            "4111111111111111,%{CREDIT_CARD_NUMBER},***",
            "visa4111111111111111,%{CREDIT_CARD_NUMBER},visa4111111111111111"
    })
    void testProcessorWithSingleWordOnly(String message, String pattern, String expected) {
        when(mockConfig.getSingleWordOnly()).thenReturn(true);
        when(mockConfig.getPatterns()).thenReturn(List.of(pattern));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @ParameterizedTest
    @CsvSource({
        "My email is abc@test.com,My email is ***",
        "My IP is 1.1.1.1,My IP is ***",
        "My IP is 1.1.1.1 and tracking id is fd55555069-e7a9-11ee4111111111111111,My IP is *** and tracking id is fd55555069-e7a9-11ee4111111111111111",
        "My IP is 1.1.1.1 and credit card number is 4111111111111111,My IP is *** and credit card number is ***",
        "My IP is 1.1.1.1 and credit card number is visa4111111111111111,My IP is *** and credit card number is visa4111111111111111"
    })
    void testProcessorWithMultiplePatternsWithSingleWordOnly(String message, String expected) {
        when(mockConfig.getSingleWordOnly()).thenReturn(true);
        when(mockConfig.getPatterns()).thenReturn(List.of("%{EMAIL_ADDRESS}", "%{IP_ADDRESS_V4}", "%{CREDIT_CARD_NUMBER}"));
        obfuscationProcessor = new ObfuscationProcessor(pluginMetrics, mockConfig, mockFactory, expressionEvaluator);

        final Record<Event> record = createRecord(message);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) obfuscationProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        Event data = editedRecords.get(0).getData();
        assertThat(data.get("message", String.class), equalTo(expected));
    }

    @Test
    void testIsReadyForShutdown() {
        assertTrue(obfuscationProcessor.isReadyForShutdown());
    }

}