/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.junit.jupiter.api.Test;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.trace.Span;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionTestUtils;

import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SumAggregateActionTest {
    private AggregateAction sumAggregateAction;

    private AggregateAction createObjectUnderTest(final SumAggregateActionConfig config) {
        return new SumAggregateAction(config);
    }

    private SumAggregateActionConfig createConfig(final String key) throws NoSuchFieldException, IllegalAccessException {
        final SumAggregateActionConfig config = new SumAggregateActionConfig();
        setField(SumAggregateActionConfig.class, config, "key", key);
        return config;
    }

    @Test
    void testSumAggregationWithEmptyGroupStateReturnsNull() throws NoSuchFieldException, IllegalAccessException {
        sumAggregateAction = createObjectUnderTest(createConfig(UUID.randomUUID().toString()));
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(Map.of());
        final AggregateActionOutput actionOutput = sumAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(actionOutput, equalTo(null));
    }

    @Test
    void testHandleEventWithMissingKeyReturnsNullEventAndDoesNotStartGroup() throws NoSuchFieldException, IllegalAccessException {
        final String key = UUID.randomUUID().toString();
        sumAggregateAction = createObjectUnderTest(createConfig(key));
        final Map<Object, Object> eventMap = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);

        final AggregateActionResponse response = sumAggregateAction.handleEvent(testEvent, aggregateActionInput);
        assertThat(response.getEvent(), equalTo(null));

        final AggregateActionOutput actionOutput = sumAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(actionOutput, equalTo(null));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void testSumAggregateRawFormat(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String key = UUID.randomUUID().toString();
        final SumAggregateActionConfig config = createConfig(key);
        setField(SumAggregateActionConfig.class, config, "outputFormat", OutputFormat.RAW);
        sumAggregateAction = createObjectUnderTest(config);

        final String identificationKey = UUID.randomUUID().toString();
        final String identificationValue = UUID.randomUUID().toString();
        final Map<Object, Object> eventMap = Collections.singletonMap(identificationKey, identificationValue);
        final Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);

        double expectedSum = 0;
        for (int i = 0; i < testCount; i++) {
            final int value = i + 1;
            expectedSum += value;
            testEvent.put(key, value);
            final AggregateActionResponse response = sumAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(response.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = sumAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        final Map<String, Object> expectedEventMap = new HashMap<>(Collections.singletonMap(identificationKey, identificationValue));
        expectedEventMap.put("aggr._sum", expectedSum);
        expectedEventMap.put(SumAggregateActionConfig.DEFAULT_COUNT_KEY, testCount);
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k, v)));
        assertThat(result.get(0).toMap(), hasKey("aggr._start_time"));
        assertThat(result.get(0).toMap(), not(hasKey("aggr._end_time")));
        assertThat(result.get(0).toMap(), not(hasKey("__exemplar")));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void testSumAggregateOTelFormat(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String key = UUID.randomUUID().toString();
        final SumAggregateActionConfig config = createConfig(key);
        final String testName = UUID.randomUUID().toString();
        setField(SumAggregateActionConfig.class, config, "metricName", testName);
        sumAggregateAction = createObjectUnderTest(config);

        final String identificationKey = UUID.randomUUID().toString();
        final String identificationValue = UUID.randomUUID().toString();
        final Map<Object, Object> eventMap = Collections.singletonMap(identificationKey, identificationValue);
        final Event testEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(eventMap)
                .build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);

        double expectedSum = 0;
        for (int i = 0; i < testCount; i++) {
            final double value = i + 0.5;
            expectedSum += value;
            testEvent.put(key, value);
            final AggregateActionResponse response = sumAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(response.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = sumAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        final Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put("value", expectedSum);
        expectedEventMap.put("name", testName);
        expectedEventMap.put("description", "Sum of the events");
        expectedEventMap.put("isMonotonic", false);
        expectedEventMap.put("aggregationTemporality", "AGGREGATION_TEMPORALITY_DELTA");
        expectedEventMap.put("unit", "1");
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k, v)));
        assertThat(result.get(0).toMap().get("attributes"), equalTo(eventMap));
        final JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));

        final List<Map<String, Object>> exemplars = (List<Map<String, Object>>) result.get(0).toMap().get("exemplars");
        assertThat(exemplars.size(), equalTo(1));
        final Map<String, Object> exemplar = exemplars.get(0);
        final Map<String, Object> attributes = (Map<String, Object>) exemplar.get("attributes");
        assertThat(attributes.get(identificationKey), equalTo(identificationValue));
        assertTrue(attributes.containsKey(key));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void testSumAggregateWithMultipleGroups(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String key = UUID.randomUUID().toString();
        sumAggregateAction = createObjectUnderTest(createConfig(key));

        final String key1 = "key-" + UUID.randomUUID();
        final String value1 = UUID.randomUUID().toString();
        final String key2 = "key-" + UUID.randomUUID();
        final String value2 = UUID.randomUUID().toString();
        final Map<Object, Object> eventMap = Collections.singletonMap(key1, value1);
        final Event testEvent = JacksonEvent.builder().withEventType("event").withData(eventMap).build();
        final Map<Object, Object> eventMap2 = Collections.singletonMap(key2, value2);
        final Event testEvent2 = JacksonEvent.builder().withEventType("event").withData(eventMap2).build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        final AggregateActionInput aggregateActionInput2 = new AggregateActionTestUtils.TestAggregateActionInput(eventMap2);

        double expectedSum1 = 0;
        double expectedSum2 = 0;
        for (int i = 0; i < testCount; i++) {
            expectedSum1 += i;
            expectedSum2 += i * 2;
            testEvent.put(key, (double) i);
            testEvent2.put(key, (double) (i * 2));
            sumAggregateAction.handleEvent(testEvent, aggregateActionInput);
            sumAggregateAction.handleEvent(testEvent2, aggregateActionInput2);
        }

        final AggregateActionOutput actionOutput1 = sumAggregateAction.concludeGroup(aggregateActionInput);
        assertThat(actionOutput1.getEvents().get(0).toMap().get("value"), equalTo(expectedSum1));

        final AggregateActionOutput actionOutput2 = sumAggregateAction.concludeGroup(aggregateActionInput2);
        assertThat(actionOutput2.getEvents().get(0).toMap().get("value"), equalTo(expectedSum2));
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 10, 20})
    void testSumAggregateOTelFormatWithStartAndEndTimesInTheEvent(final int testCount) throws NoSuchFieldException, IllegalAccessException {
        final String key = UUID.randomUUID().toString();
        sumAggregateAction = createObjectUnderTest(createConfig(key));

        final String identificationKey = UUID.randomUUID().toString();
        final String identificationValue = UUID.randomUUID().toString();
        final Instant testTime = Instant.ofEpochSecond(Instant.now().getEpochSecond());
        final Map<Object, Object> eventMap = Collections.singletonMap(identificationKey, identificationValue);
        final Event testEvent = JacksonEvent.builder().withEventType("event").withData(eventMap).build();
        final AggregateActionInput aggregateActionInput = new AggregateActionTestUtils.TestAggregateActionInput(eventMap);
        final Random random = new Random();

        double expectedSum = 0;
        for (int i = 0; i < testCount; i++) {
            final double value = i + 1;
            expectedSum += value;
            testEvent.put(key, value);
            // the last event reports the earliest start time and the latest end time seen so far,
            // forcing both the "earlier start" and "later end" group update branches to run
            final Instant sTime = (i == testCount - 1) ? testTime : testTime.plusSeconds(10 + random.nextInt(5));
            final Instant eTime = (i == testCount - 1) ? testTime.plusSeconds(200) : testTime.plusSeconds(50 + random.nextInt(45));
            testEvent.put("aggr._start_time", (i % 2 == 0) ? sTime : sTime.toString());
            testEvent.put("aggr._end_time", (i % 2 == 0) ? eTime : eTime.toString());
            final AggregateActionResponse response = sumAggregateAction.handleEvent(testEvent, aggregateActionInput);
            assertThat(response.getEvent(), equalTo(null));
        }

        final AggregateActionOutput actionOutput = sumAggregateAction.concludeGroup(aggregateActionInput);
        final List<Event> result = actionOutput.getEvents();
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).toMap().get("value"), equalTo(expectedSum));
        assertThat(result.get(0).get("startTime", String.class), equalTo(testTime.toString()));
        assertThat(result.get(0).get("time", String.class), equalTo(testTime.plusSeconds(200).toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"__exemplar", "aggr._sum", "aggr._start_time", "aggr._end_time"})
    void testConstructorThrowsWhenCountKeyCollidesWithReservedKey(final String reservedKey) throws NoSuchFieldException, IllegalAccessException {
        final SumAggregateActionConfig config = createConfig(UUID.randomUUID().toString());
        setField(SumAggregateActionConfig.class, config, "countKey", reservedKey);

        assertThrows(InvalidPluginConfigurationException.class, () -> new SumAggregateAction(config));
    }

    @Test
    void testCreateExemplarWithSpanEventUsesSpanIdAndTraceId() throws NoSuchFieldException, IllegalAccessException {
        final SumAggregateAction action = new SumAggregateAction(createConfig(UUID.randomUUID().toString()));
        final Span spanEvent = mock(Span.class);
        final String spanId = UUID.randomUUID().toString();
        final String traceId = UUID.randomUUID().toString();
        when(spanEvent.getSpanId()).thenReturn(spanId);
        when(spanEvent.getTraceId()).thenReturn(traceId);
        when(spanEvent.toMap()).thenReturn(Map.of());

        final Exemplar exemplar = action.createExemplar(spanEvent, 42.0);

        assertThat(exemplar.getSpanId(), equalTo(spanId));
        assertThat(exemplar.getTraceId(), equalTo(traceId));
        assertThat(exemplar.getValue(), equalTo(42.0));
    }
}
