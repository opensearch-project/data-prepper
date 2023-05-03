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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;

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
    void testCountAggregateOTelFormat(int testCount) {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
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
        Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put("value", (double)testCount);
        expectedEventMap.put("name", "count");
        expectedEventMap.put("description", "Number of events");
        expectedEventMap.put("isMonotonic", true);
        expectedEventMap.put("aggregationTemporality", "AGGREGATION_TEMPORALITY_DELTA");
        expectedEventMap.put("unit", "1");
        expectedEventMap.forEach((k, v) -> assertThat(result.get(0).toMap(), hasEntry(k,v)));
        assertThat(result.get(0).toMap().get("attributes"), equalTo(eventMap));
        JacksonMetric metric = (JacksonMetric) result.get(0);
        assertThat(metric.toJsonString().indexOf("attributes"), not(-1));
        assertThat(result.get(0).toMap(), hasKey("startTime"));
        assertThat(result.get(0).toMap(), hasKey("time"));
    }
}
