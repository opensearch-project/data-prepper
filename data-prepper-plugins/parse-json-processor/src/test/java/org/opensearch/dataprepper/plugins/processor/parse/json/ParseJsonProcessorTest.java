/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.opensearch.dataprepper.plugins.processor.parse.CommonParseConfig;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ParseJsonProcessorTest {
    private static final String DEEPLY_NESTED_KEY_NAME = "base";

    private final ObjectMapper objectMapper = new ObjectMapper();

    protected CommonParseConfig processorConfig;

    @Mock
    private ParseJsonProcessorConfig jsonProcessorConfig;

    @Mock
    protected PluginMetrics pluginMetrics;

    @Mock
    protected ExpressionEvaluator expressionEvaluator;

    @Mock
    protected HandleFailedEventsOption handleFailedEventsOption;

    @Mock
    protected Counter processingFailuresCounter;

    @Mock
    protected Counter parseErrorsCounter;

    protected AbstractParseProcessor parseJsonProcessor;
    private final EventFactory testEventFactory = TestEventFactory.getTestEventFactory();
    protected final EventKeyFactory testEventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @BeforeEach
    public void setup() {
        processorConfig = jsonProcessorConfig;
        ParseJsonProcessorConfig defaultConfig = new ParseJsonProcessorConfig();
        when(processorConfig.getSource()).thenReturn(defaultConfig.getSource());
        when(processorConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        when(processorConfig.getPointer()).thenReturn(defaultConfig.getPointer());
        when(processorConfig.getParseWhen()).thenReturn(null);
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(true);

        when(pluginMetrics.counter("recordsIn")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("recordsOut")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("processingFailures")).thenReturn(processingFailuresCounter);
        lenient().when(pluginMetrics.counter("parseErrors")).thenReturn(parseErrorsCounter);
        when(processorConfig.getHandleFailedEventsOption()).thenReturn(handleFailedEventsOption);
    }

    protected AbstractParseProcessor createObjectUnderTest() {
        return new ParseJsonProcessor(pluginMetrics, jsonProcessorConfig, expressionEvaluator, testEventKeyFactory);
    }

    @Test
    void invalid_parse_when_throws_InvalidPluginConfigurationException() {
        final String parseWhen = UUID.randomUUID().toString();

        when(processorConfig.getParseWhen()).thenReturn(parseWhen);
        when(expressionEvaluator.isValidExpressionStatement(parseWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_simple_depth_value_1() throws Exception {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(source);
        when(processorConfig.getDepth()).thenReturn(1);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used
	
        Map<String, Object> data = Map.of("key1", "value1", "key2", 1, "key3", Map.of("key5", Map.of("key6", "value6")));
        Map<String, Object> expectedResult = Map.of("key1", "value1", "key2", 1, "key3", "{\"key5\":{\"key6\":\"value6\"}}");
        final String serializedMessage = objectMapper.writeValueAsString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);
        assertThatKeyEquals(parsedEvent, source, expectedResult);
    }

    @Test
    void test_replace_invalid_key_characters() throws Exception {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(source);
        when(processorConfig.getDepth()).thenReturn(0);
        when(processorConfig.getNormalizeKeys()).thenReturn(true);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used
        Map<String, Object> data = Map.of("key^2", 1, "key%5", Map.of("key&6", "value6"));
        // % is valid in event keys, so key%5 is not normalized; ^ and & are invalid and replaced with _
        Map<String, Object> expectedResult = Map.of("key_2", 1, "key%5", Map.of("key_6", "value6"));
        final String serializedMessage = objectMapper.writeValueAsString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);
        assertThatKeyEquals(parsedEvent, source, expectedResult);
    }

    @Test
    void test_simple_depth_value_2() throws Exception {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(source);
        when(processorConfig.getDepth()).thenReturn(2);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used
	
	Map<String, Object> data = Map.of("key1", "value1", "key2", 1, "key3", Map.of("key4", 4, "key5", Map.of("key6", "value6")));
	Map<String, Object> expectedResult = Map.of("key1", "value1", "key2", 1, "key3", Map.of("key4", 4, "key5", "{\"key6\":\"value6\"}"));
        final String serializedMessage = objectMapper.writeValueAsString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);
        assertThatKeyEquals(parsedEvent, source, expectedResult);
    }

    @ParameterizedTest
    @ValueSource(ints = {0,1,2,3,4,5,6,7,8,9,10})
    void test_depth_option_with_same_source_and_destination(int depth) throws Exception {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(source);
        when(processorConfig.getDepth()).thenReturn(depth);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        Map<String, Object> mapValue = new HashMap<>();
        Map<String, Object> prevMap = null;
        Object expectedResult = null;
        // Create a map with 12 nested levels
        for (int i = 11; i >= 0; i--) {
            Map<String, Object> m = (prevMap == null) ?
                    Map.of("key" + i, i, "key" + (100 + i), "value" + i) :
                    Map.of("key" + i, i, "key" + (100 + i), "value" + i, "key"+(1000+i), prevMap);
            if (i == depth) {
                expectedResult = (depth == 0) ? m : objectMapper.writeValueAsString(m);
            } else if (i < depth) {
                expectedResult = Map.of("key" + i, i, "key" + (100 + i), "value" + i, "key"+(1000+i), expectedResult);
            }
            prevMap = m;
        }

        mapValue = prevMap;
        final String serializedMessage = objectMapper.writeValueAsString(mapValue);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);
        assertThatKeyEquals(parsedEvent, source, expectedResult);

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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_dataFieldEqualToRootField_then_notOverwritesOriginalFields() {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Map<String, Object> data = Map.of(source,"value_that_will_not_be_overwritten");

        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, source, "{\"root_source\":\"value_that_will_not_be_overwritten\"}");

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_dataFieldEqualToDestinationField_then_notOverwritesOriginalFields() {
        final String source = "root_source";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(source);  // write back to source
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Map<String, Object> data = Map.of("key","value");

        final String serializedMessage = convertMapToJSONString(data);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, source, "{\"key\":\"value\"}");
        assertThat(parsedEvent.containsKey("key"), equalTo(false));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_valueIsEmpty_then_notParsed() {
        parseJsonProcessor = createObjectUnderTest();

        final Map<String, Object> emptyData = Collections.singletonMap("key",""); // invalid JSON

        final String serializedMessage = convertMapToJSONString(emptyData);
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThatKeyEquals(parsedEvent, processorConfig.getSource(), serializedMessage);
        assertThat(parsedEvent.toMap().size(), equalTo(1));

        verify(parseErrorsCounter).increment();
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_deleteSourceFlagEnabled() {
        when(processorConfig.isDeleteSourceRequested()).thenReturn(true);
        parseJsonProcessor = createObjectUnderTest();

        final String key = "key";
        final ArrayList<String> value = new ArrayList<>(List.of("Element0","Element1","Element2"));
        final String jsonArray = "{\"key\":[\"Element0\",\"Element1\",\"Element2\"]}";
        final Event parsedEvent = createAndParseMessageEvent(jsonArray);

        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(false));
        assertThat(parsedEvent.get(key, ArrayList.class), equalTo(value));
        final String pointerToFirstElement = key + "/0";
        assertThat(parsedEvent.get(pointerToFirstElement, String.class), equalTo(value.get(0)));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);
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
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(whenCondition, testEvent.getData())).thenReturn(false);
        parseJsonProcessor = createObjectUnderTest(); // need to recreate so that new config options are used

        final Event parsedEvent = createAndParseMessageEvent(testEvent);

        assertThat(parsedEvent.toMap(), equalTo(testEvent.getData().toMap()));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(parseErrorsCounter);
        verifyNoInteractions(handleFailedEventsOption);

    }

    @Test
    void test_tags_when_json_parse_fails() {
        when(handleFailedEventsOption.shouldLog()).thenReturn(true);

        final String source = "different_source";
        final String destination = "destination_key";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(destination);
        final String whenCondition = UUID.randomUUID().toString();
        when(processorConfig.getParseWhen()).thenReturn(whenCondition);
        List<String> testTags = List.of("tag1", "tag2");
        when(processorConfig.getTagsOnFailure()).thenReturn(testTags);
        final Record<Event> testEvent = createMessageEvent("{key:}");
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(whenCondition, testEvent.getData())).thenReturn(true);
        parseJsonProcessor = createObjectUnderTest();

        final Event parsedEvent = createAndParseMessageEvent(testEvent);
        assertTrue(parsedEvent.getMetadata().hasTags(testTags));

        verify(parseErrorsCounter).increment();
    }

    @Test
    void when_evaluate_conditional_throws_RuntimeException_events_are_not_dropped() {
        when(handleFailedEventsOption.shouldLog()).thenReturn(true);

        final String source = "different_source";
        final String destination = "destination_key";
        when(processorConfig.getSource()).thenReturn(source);
        when(processorConfig.getDestination()).thenReturn(destination);
        final String whenCondition = UUID.randomUUID().toString();
        when(processorConfig.getParseWhen()).thenReturn(whenCondition);
        final Map<String, Object> data = Collections.singletonMap("key", "value");
        final String serializedMessage = convertMapToJSONString(data);
        final Record<Event> testEvent = createMessageEvent(serializedMessage);
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(whenCondition, testEvent.getData())).thenThrow(RuntimeException.class);
        parseJsonProcessor = createObjectUnderTest();

        final Event parsedEvent = createAndParseMessageEvent(testEvent);

        assertThat(parsedEvent.toMap(), equalTo(testEvent.getData().toMap()));

        verify(processingFailuresCounter).increment();
        verifyNoInteractions(parseErrorsCounter);
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
     * @param messageMap source key value map
     * @return serialized string representation of the map
     */
    private String convertMapToJSONString(final Map<String, Object> messageMap) {
        final String replaceEquals = messageMap.toString().replace("=",":");
        return replaceEquals.replaceAll("(\\w+)", "\"$1\"");
    }

    /**
     * Creates a Map that maps a single key to a value nested numberOfLayers layers deep.
     * @param numberOfLayers indicates the depth of layers count
     * @return a Map representing the nested structure
     */
    private Map<String, Object> constructArbitrarilyDeepJsonMap(final int numberOfLayers) {
        return Collections.singletonMap(DEEPLY_NESTED_KEY_NAME,deepJsonMapHelper(0,numberOfLayers));
    }

    private Object deepJsonMapHelper(final int currentLayer, final int numberOfLayers) {
        if (currentLayer >= numberOfLayers) return "value";

        final String key = "key" + currentLayer;
        return Collections.singletonMap(key, deepJsonMapHelper(currentLayer+1, numberOfLayers));
    }

    protected Event createAndParseMessageEvent(final String message) {
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
        return new Record<>(testEventFactory.eventBuilder(EventBuilder.class).withData(data).build());
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
