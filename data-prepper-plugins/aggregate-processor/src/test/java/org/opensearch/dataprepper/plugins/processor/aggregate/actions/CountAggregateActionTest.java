/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.junit.jupiter.api.extension.ExtendWith; 
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.time.Instant;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

@ExtendWith(MockitoExtension.class)
public class CountAggregateActionTest {
    AggregateActionInput aggregateActionInput;

    private AggregateAction countAggregateAction;

    private AggregateAction createObjectUnderTest(CountAggregateActionConfig config) {
        return new CountAggregateAction(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void testCountAggregate(int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String testName = UUID.randomUUID().toString();
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        countAggregateAction = createObjectUnderTest(countAggregateActionConfig);
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String dataKey = UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key, value);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        for (int i = 0; i < testCount; i++) { 
            testEvent.put(dataKey, UUID.randomUUID().toString());
            final AggregateActionResponse aggregateActionResponse = countAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = countAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        Map<String, Object> expectedEventMap = new HashMap<>(Collections.singletonMap(key, value));
        expectedEventMap.put(CountAggregateActionConfig.DEFAULT_COUNT_KEY, testCount);
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k,v)));
        assertThat(result.get(0).toMap(), hasKey(CountAggregateActionConfig.DEFAULT_START_TIME_KEY));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void testCountAggregateOTelFormat(int testCount) throws NoSuchFieldException, IllegalAccessException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        final String testName = UUID.randomUUID().toString();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "name", testName);
        countAggregateAction = createObjectUnderTest(countAggregateActionConfig);
        final String key1 = "key-"+UUID.randomUUID().toString();
        final String value1 = UUID.randomUUID().toString();
        final String dataKey1 = "datakey-"+UUID.randomUUID().toString();
        final String key2 = "key-"+UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final String dataKey2 = "datakey-"+UUID.randomUUID().toString();
        Map<Object, Object> eventMap = Collections.singletonMap(key1, value1);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        Map<Object, Object> eventMap2 = Collections.singletonMap(key2, value2);
        JacksonEvent testEvent2 = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap2)
                .build();
        AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        AggregateActionInput aggregateActionInput2 = new AggregateActionTestUtils.TestAggregateActionInput(eventMap2);
        for (int i = 0; i < testCount; i++) { 
            testEvent.put(dataKey1, UUID.randomUUID().toString());
            testEvent2.put(dataKey2, UUID.randomUUID().toString());
            AggregateActionResponse aggregateActionResponse = countAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
            aggregateActionResponse = countAggregateAction.handleEvent(testEvent2, aggregateActionInput2);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        AggregateActionOutput actionOutput = countAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put("value", (double)testCount);
        expectedEventMap.put("name", "count");
        expectedEventMap.put("description", "Number of events");
        expectedEventMap.put("isMonotonic", true);
        expectedEventMap.put("aggregationTemporality", "AGGREGATION_TEMPORALITY_DELTA");
        expectedEventMap.put("unit", "1");
        expectedEventMap.put("name", testName);
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k,v)));
        assertThat(result.get(0).toMap().get("attributes"), equalTo(eventMap));
        JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));
        List<Map<String, Object>> exemplars = (List <Map<String, Object>>)result.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(1));
        Map<String, Object> exemplar = exemplars.get(0);
        Map<String, Object> attributes = (Map<String, Object>)exemplar.get("attributes");
        assertThat(attributes.get(key1), equalTo(value1));
        assertTrue(attributes.containsKey(dataKey1));

        actionOutput = countAggregateAction.concludeGroup(aggregateActionInput2);
        final List<Event> result2 = actionOutput.getEvents();
        assertThat(result2.size(), equalTo(1));

        exemplars = (List <Map<String, Object>>)result2.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(1));
        exemplar = exemplars.get(0);
        attributes = (Map<String, Object>)exemplar.get("attributes");
        assertThat(attributes.get(key2), equalTo(value2));
        assertTrue(attributes.containsKey(dataKey2));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void testCountAggregateOTelFormatWithStartAndEndTimesInTheEvent(int testCount) {
        CountAggregateActionConfig mockConfig = mock(CountAggregateActionConfig.class);
        when(mockConfig.getCountKey()).thenReturn(CountAggregateActionConfig.DEFAULT_COUNT_KEY);
        final String testName = UUID.randomUUID().toString();
        when(mockConfig.getName()).thenReturn(testName);
        String startTimeKey = UUID.randomUUID().toString();
        String endTimeKey = UUID.randomUUID().toString();
        when(mockConfig.getStartTimeKey()).thenReturn(startTimeKey);
        when(mockConfig.getEndTimeKey()).thenReturn(endTimeKey);
        when(mockConfig.getOutputFormat()).thenReturn(OutputFormat.OTEL_METRICS.toString());
        countAggregateAction = createObjectUnderTest(mockConfig);
        final String key1 = "key-"+UUID.randomUUID().toString();
        final String value1 = UUID.randomUUID().toString();
        final String dataKey1 = "datakey-"+UUID.randomUUID().toString();
        final String key2 = "key-"+UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final String dataKey2 = "datakey-"+UUID.randomUUID().toString();
        final Instant testTime = Instant.ofEpochSecond(Instant.now().getEpochSecond());
        Map<Object, Object> eventMap = Collections.singletonMap(key1, value1);
        Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        Map<Object, Object> eventMap2 = Collections.singletonMap(key2, value2);
        JacksonEvent testEvent2 = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap2)
                .build();
        AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        AggregateActionInput aggregateActionInput2 = new AggregateActionTestUtils.TestAggregateActionInput(eventMap2);
        Random random = new Random();
        for (int i = 0; i < testCount; i++) {
            testEvent.put(dataKey1, UUID.randomUUID().toString());
            Instant sTime = (i == 0) ? testTime : testTime.plusSeconds(random.nextInt(5));
            Instant eTime = (i == testCount-1) ? testTime.plusSeconds(100) : testTime.plusSeconds (50+random.nextInt(45));
            testEvent.put(startTimeKey, sTime);
            testEvent.put(endTimeKey,  eTime);
            testEvent2.put(dataKey2, UUID.randomUUID().toString());
            testEvent2.put(startTimeKey, sTime.toString());
            testEvent2.put(endTimeKey,  eTime.toString());
            AggregateActionResponse aggregateActionResponse = countAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
            aggregateActionResponse = countAggregateAction.handleEvent(testEvent2, aggregateActionInput2);
            assertThat(aggregateActionResponse.getEvent(), equalTo(null));
        }

        AggregateActionOutput actionOutput = countAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put("value", (double)testCount);
        expectedEventMap.put("name", testName);
        expectedEventMap.put("description", "Number of events");
        expectedEventMap.put("isMonotonic", true);
        expectedEventMap.put("aggregationTemporality", "AGGREGATION_TEMPORALITY_DELTA");
        expectedEventMap.put("unit", "1");
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k,v)));
        assertThat(result.get(0).toMap().get("attributes"), equalTo(eventMap));
        JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        assertThat(result.get(0).get("startTime", String.class), equalTo(testTime.toString()));
        assertThat(result.get(0).get("time", String.class), equalTo(testTime.plusSeconds(100).toString()));

        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));
        List<Map<String, Object>> exemplars = (List <Map<String, Object>>)result.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(1));
        Map<String, Object> exemplar = exemplars.get(0);
        Map<String, Object> attributes = (Map<String, Object>)exemplar.get("attributes");
        assertThat(attributes.get(key1), equalTo(value1));
        assertTrue(attributes.containsKey(dataKey1));

        actionOutput = countAggregateAction.concludeGroup(aggregateActionInput2);
        final List<Event> result2 = actionOutput.getEvents();
        assertThat(result2.size(), equalTo(1));

        exemplars = (List <Map<String, Object>>)result2.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(1));
        exemplar = exemplars.get(0);
        attributes = (Map<String, Object>)exemplar.get("attributes");
        assertThat(attributes.get(key2), equalTo(value2));
        assertTrue(attributes.containsKey(dataKey2));
    }
}
