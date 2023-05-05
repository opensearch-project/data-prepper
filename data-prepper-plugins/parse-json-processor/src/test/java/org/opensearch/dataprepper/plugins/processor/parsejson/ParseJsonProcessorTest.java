/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parsejson;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.util.Map.entry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ParseJsonProcessorTest {
    private static final String DEEPLY_NESTED_KEY_NAME = "base";

    @Mock
    private ParseJsonProcessorConfig processorConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private ParseJsonProcessor parseJsonProcessor;

    @BeforeEach
    void setup() {
        ParseJsonProcessorConfig defaultConfig = new ParseJsonProcessorConfig();
        when(processorConfig.getSource()).thenReturn(defaultConfig.getSource());
        when(processorConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        when(processorConfig.getPointer()).thenReturn(defaultConfig.getPointer());
        when(processorConfig.getParseWhen()).thenReturn(null);
    }

    private ParseJsonProcessor createObjectUnderTest() {
        return new ParseJsonProcessor(pluginMetrics, processorConfig, expressionEvaluator);
    }

    @Test
    void test_when_differentSourceAndDestination_then_processorParsesCorrectly() {
        final String source = "different_source";
        final String destination = "destination_key";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(destination);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Map<String, Object> data = Collections.singletonMap("key", "value");
        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.containsKey(source), equalTo(true));
        assertThat(parsedEvent.containsKey(destination), equalTo(true));

        assertThatFirstMapIsSubsetOfSecondMap(data, parsedEvent.get(destination, Map.class));
    }

    @Test
    void test_when_dataFieldEqualToRootField_then_overwritesOriginalFields() {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Map<String, Object> data = Map.ofEntries(
                entry(source,"value_that_will_overwrite_source"),
                entry("key","value")
        );

        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, source, "value_that_will_overwrite_source");
        assertThatKeyEquals(parsedEvent, "key", "value");
    }

    @Test
    void test_when_valueIsEmpty_then_notParsed() {
        parseJsonProcessor = createObjectUnderTest();

        final Map<String, Object> emptyData = Collections.singletonMap("key",""); // invalid JSON

        final String serializedMessage = convertMapToJSONString(emptyData);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, processorConfig.getSource(), serializedMessage);
        assertThat(parsedEvent.toMap().size(), equalTo(1));
    }

    @Test
    void test_when_deeplyNestedFieldInRoot_then_canReachDeepestLayer() {
        parseJsonProcessor = createObjectUnderTest();

        final int numberOfLayers = 200;
        final Map<String, Object> messageMap = constructArbitrarilyDeepJsonMap(numberOfLayers);
        final String serializedMessage = convertMapToJSONString(messageMap);

        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, DEEPLY_NESTED_KEY_NAME, messageMap.get(DEEPLY_NESTED_KEY_NAME));
        final String jsonPointerToValue = constructDeeplyNestedJsonPointer(numberOfLayers);
        assertThat(parsedEvent.get(jsonPointerToValue, String.class), equalTo("value"));
    }

    @Test
    void test_when_deeplyNestedFieldInKey_then_canReachDeepestLayer() {
        final String destination = "destination_key";
        when(processorConfig.getDestination()).thenReturn(destination);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final int numberOfLayers = 20;
        final Map<String, Object> messageMap = constructArbitrarilyDeepJsonMap(numberOfLayers);
        final String serializedMessage = convertMapToJSONString(messageMap);

        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        final String completeDeeplyNestedKeyName = destination + "/" + DEEPLY_NESTED_KEY_NAME;
        assertThatKeyEquals(parsedEvent, completeDeeplyNestedKeyName, messageMap.get(DEEPLY_NESTED_KEY_NAME));
        final String jsonPointerToValue = destination + constructDeeplyNestedJsonPointer(numberOfLayers);

        assertThat(parsedEvent.get(jsonPointerToValue, String.class), equalTo("value"));
    }

    @Test
    void test_when_nestedJSONArray_then_parsedIntoArrayAndIndicesAccessible() {
        parseJsonProcessor = createObjectUnderTest();

        final String key = "key";
        final ArrayList<String> value = new ArrayList<>(List.of("Element0","Element1","Element2"));
        final String jsonArray = "{\"key\":[\"Element0\",\"Element1\",\"Element2\"]}";
        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(true));
        assertThat(parsedEvent.get(key, ArrayList.class), equalTo(value));
        final String pointerToFirstElement = key + "/0";
        assertThat(parsedEvent.get(pointerToFirstElement, String.class), equalTo(value.get(0)));
    }

    @Test
    void test_when_nestedJSONArrayOfJSON_then_parsedIntoArrayAndIndicesAccessible() {
        parseJsonProcessor = createObjectUnderTest();

        final String key = "key";
        final ArrayList<Map<String, Object>> value = new ArrayList<>(List.of(Collections.singletonMap("key0","value0"),
                Collections.singletonMap("key1","value1")));
        final String jsonArray = "{\"key\":[{\"key0\":\"value0\"},{\"key1\":\"value1\"}]}";

        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(true));
        assertThat(parsedEvent.get(key, ArrayList.class), equalTo(value));

        final String pointerToInternalValue = key + "/0/key0";
        assertThat(parsedEvent.get(pointerToInternalValue, String.class), equalTo("value0"));
    }

    @Test
    void test_when_nestedJSONArrayOfJSONAndPointer_then_parsedIntoValue() {
        final String pointer = "/key/0/key0";
        when(processorConfig.getPointer()).thenReturn(pointer);
        parseJsonProcessor = createObjectUnderTest();

        final ArrayList<Map<String, Object>> value = new ArrayList<>(List.of(Collections.singletonMap("key0","value0"),
                Collections.singletonMap("key1","value1")));
        final String jsonArray = "{\"key\":[{\"key0\":\"value0\"},{\"key1\":\"value1\"}]}";

        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(true));

        assertThat(parsedEvent.get("key0", String.class), equalTo("value0"));
        assertThat(parsedEvent.containsKey("key1"),equalTo(false));
    }

    @Test
    void test_when_nestedJSONArrayAndIndexPointer_then_parsedIntoArrayAndIndicesAccessible() {
        final String pointer = "/key/0/";
        when(processorConfig.getPointer()).thenReturn(pointer);
        parseJsonProcessor = createObjectUnderTest();
        final ArrayList<String> value = new ArrayList<>(List.of("Element0","Element1","Element2"));
        final String jsonArray = "{\"key\":[\"Element0\",\"Element1\",\"Element2\"]}";
        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(true));
        assertThat(parsedEvent.get("key.0", String.class), equalTo(value.get(0)));
    }

    @Test
    void test_when_pointerKeyAlreadyPresentInEvent_then_usesAbsolutePath() {
        final String pointer = "/log/s3/";
        when(processorConfig.getPointer()).thenReturn(pointer);
        parseJsonProcessor = createObjectUnderTest();
        final Map<String, Object> s3Data = Collections.singletonMap("bucket","sampleBucket");
        final Map<String, Object> data = new HashMap<>();
        data.put("message", "{\"log\": {\"s3\": {\"data\":\"sample data\"}}}");
        data.put("s3", s3Data);

        Record<Event> record = buildRecordWithEvent(data);
        final Event parsedEvent = ((List<Record<Event>>) parseJsonProcessor.doExecute(Collections.singletonList(record)))
                .get(0).getData();

        assertThatKeyEquals(parsedEvent, "message", data.get("message"));
        assertThatKeyEquals(parsedEvent, "s3", data.get("s3"));

        assertThatKeyEquals(parsedEvent, "log.s3", Collections.singletonMap("data", "sample data"));
    }

    @Test
    void test_when_nestedDestinationField_then_writesToNestedDestination() {
        final String destination = "/destination/nested";
        when(processorConfig.getDestination()).thenReturn(destination);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used
        final Map<String, Object> data = Collections.singletonMap("key", "value");
        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        final String location = destination + "/key";

        assertThat(parsedEvent.get(location, String.class), equalTo("value"));
        assertThat(parsedEvent.get(destination, Map.class), equalTo(data));
    }

    @Test
    void test_when_invalidPointer_then_logsErrorAndParsesEntireEvent() {

        final String pointer = "key/10000";
        when(processorConfig.getPointer()).thenReturn(pointer);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final ArrayList<String> value = new ArrayList<>(List.of("Element0","Element1","Element2"));
        final String jsonArray = "{\"key\":[\"Element0\",\"Element1\",\"Element2\"]}";

        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThatKeyEquals(parsedEvent, processorConfig.getSource(), jsonArray);
        assertThatKeyEquals(parsedEvent, "key", value);
    }

    @Test
    void test_when_multipleChildren_then_allAreParsedOut() {
        parseJsonProcessor = createObjectUnderTest();

        final Map<String, Object> data = Collections.singletonMap("key", "{inner1:value1,inner2:value2}");
        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, "key/inner1", "value1");
        assertThatKeyEquals(parsedEvent, "key/inner2", "value2");
    }

    @Test
    void test_when_condition_skips_processing_when_evaluates_to_false() {
        final String source = "different_source";
        final String destination = "destination_key";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(destination);
        final String whenCondition = UUID.randomUUID().toString();
        when(processorConfig.getParseWhen()).thenReturn(whenCondition);
        final Map<String, Object> data = Collections.singletonMap("key", "value");
        final String serializedMessage = convertMapToJSONString(data);
        final Record<Event> testEvent = createMessageEvent(serializedMessage);
        when(expressionEvaluator.evaluate(whenCondition, testEvent.getData())).thenReturn(false);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Event parsedEvent = createAndParseMessageEvent(testEvent);

        assertThat(parsedEvent.toMap(), equalTo(testEvent.getData().toMap()));

    }

    private String constructDeeplyNestedJsonPointer(final int numberOfLayers) {
        String pointer = "/" + DEEPLY_NESTED_KEY_NAME;
        for (int layer = 0; layer < numberOfLayers; layer++) {
            pointer += "/key" + layer;
        }
        return pointer;
    }

    /**
     * Naive serialization that converts every = to : and wraps every word with double quotes (no error handling or input validation).
     * @param messageMap
     * @return
     */
    private String convertMapToJSONString(final Map<String, Object> messageMap) {
        final String replaceEquals = messageMap.toString().replace("=",":");
        final String addQuotes = replaceEquals.replaceAll("(\\w+)", "\"$1\""); // wrap every word in quotes
        return addQuotes;
    }

    /**
     * Creates a Map that maps a single key to a value nested numberOfLayers layers deep.
     * @param numberOfLayers
     * @return
     */
    private Map<String, Object> constructArbitrarilyDeepJsonMap(final int numberOfLayers) {
        final Map<String, Object> result = Collections.singletonMap(DEEPLY_NESTED_KEY_NAME,deepJsonMapHelper(0,numberOfLayers));
        return result;
    }

    private Object deepJsonMapHelper(final int currentLayer, final int numberOfLayers) {
        if (currentLayer >= numberOfLayers) return "value";

        final String key = "key" + currentLayer;
        return Collections.singletonMap(key, deepJsonMapHelper(currentLayer+1, numberOfLayers));
    }

    private Event createAndParseMessageEvent(final String message) {
        final Record<Event> eventUnderTest = createMessageEvent(message);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) parseJsonProcessor.doExecute(
                Collections.singletonList(eventUnderTest));
        return editedEvents.get(0).getData();
    }

    private Event createAndParseMessageEvent(final Record<Event> inputEvent) {
        final List<Record<Event>> editedEvents = (List<Record<Event>>) parseJsonProcessor.doExecute(
                Collections.singletonList(inputEvent));
        return editedEvents.get(0).getData();
    }


    private Record<Event> createMessageEvent(final String message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put(processorConfig.getSource(), message);
        return buildRecordWithEvent(eventData);
    }

    private Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private void assertThatKeyEquals(final Event parsedEvent, final String key, final Object value) {
        assertThat(parsedEvent.containsKey(key), equalTo(true));
        assertThat(parsedEvent.get(key, Object.class), equalTo(value));
    }

    private void assertThatFirstMapIsSubsetOfSecondMap(final Map<String, Object> subset, final Map<String, Object> secondMap) {
        assertThat(Objects.nonNull(subset), equalTo(true));
        assertThat(Objects.nonNull(secondMap), equalTo(true));

        assertThat((subset.size() <= secondMap.size()), equalTo(true));

        for (Map.Entry<String, Object> entry : subset.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            assertThat(secondMap.containsKey(key), equalTo(true));
            assertThat(secondMap.get(key), equalTo(value));
        }
    }
}