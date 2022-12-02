/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RemoveDuplicatesAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PutAllAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIn.in;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * These integration tests are executing concurrent code that is inherently difficult to test, and even more difficult to recreate a failed test.
 * If any of these tests are to fail, please create a bug report as a GitHub issue with the details of the failed test.
 */
@ExtendWith(MockitoExtension.class)
public class AggregateProcessorIT {

    private static final int testValue = 1;
    private static final int NUM_EVENTS_PER_BATCH = 200;
    private static final int NUM_UNIQUE_EVENTS_PER_BATCH = 8;
    private static final int NUM_THREADS = 100;
    private static final int GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE = 2;
    @Mock
    private AggregateProcessorConfig aggregateProcessorConfig;

    private AggregateAction aggregateAction;
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private Collection<Record<Event>> eventBatch;
    private ConcurrentLinkedQueue<Map<String, Object>> aggregatedResult;
    private Set<Map<String, Object>> uniqueEventMaps;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginModel actionConfiguration;

    @BeforeEach
    void setup() {
        aggregatedResult = new ConcurrentLinkedQueue<>();
        uniqueEventMaps = new HashSet<>();

        final List<String> identificationKeys = new ArrayList<>();
        identificationKeys.add("firstRandomNumber");
        identificationKeys.add("secondRandomNumber");
        identificationKeys.add("thirdRandomNumber");


        eventBatch = getBatchOfEvents(false);

        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(identificationKeys);
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
    }

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
    }

    @RepeatedTest(value = 10)
    void aggregateWithNoConcludingGroupsReturnsExpectedResult() throws InterruptedException {
        aggregateAction = new RemoveDuplicatesAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(1000));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }

        for (final Map<String, Object> eventMap : aggregatedResult) {
            assertThat(eventMap, in(uniqueEventMaps));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithConcludingGroupsOnceReturnsExpectedResult() throws InterruptedException {
        aggregateAction = new RemoveDuplicatesAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }

        for (final Map<String, Object> eventMap : aggregatedResult) {
            assertThat(eventMap, in(uniqueEventMaps));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithPutAllActionAndCondition() throws InterruptedException {
        aggregateAction = new PutAllAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        String condition = "/firstRandomNumber < 100";
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
        int count = 0;
        for (Record<Event> record: eventBatch) {
            Event event = record.getData();
            boolean value = (count % 2 == 0) ? true : false;
            when(expressionEvaluator.evaluate(condition, event)).thenReturn(value);
            if (!value) {
                uniqueEventMaps.remove(event.toMap());
            }
            count++;
        }
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                for (final Record<Event> record : recordsOut) {
                    final Map<String, Object> map = record.getData().toMap();
                    aggregatedResult.add(map);
                }
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat(aggregatedResult.size(), equalTo(NUM_UNIQUE_EVENTS_PER_BATCH/2));

        for (final Map<String, Object> uniqueEventMap : uniqueEventMaps) {
            assertThat(aggregatedResult, hasItem(uniqueEventMap));
        }
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateAction() throws InterruptedException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        eventBatch = getBatchOfEvents(true);

        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                countDownLatch.countDown();
            });
        }
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        Map<String, Object> expectedEventMap = new HashMap<>(getEventMap(testValue));
        expectedEventMap.put(DEFAULT_COUNT_KEY, NUM_THREADS * NUM_EVENTS_PER_BATCH);

        Record<Event> record = (Record<Event>)results.toArray()[0];
        assertThat(expectedEventMap, equalTo(record.getData().toMap()));
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateActionWithCondition() throws InterruptedException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        final String condition = "/firstRandomNumber < 100";
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
        int count = 0;
        eventBatch = getBatchOfEvents(true);
        Record<Event> rec = (Record<Event>)eventBatch.toArray()[0];
        for (Record<Event> record: eventBatch) {
            Event event = record.getData();
            boolean value = (count % 2 == 0) ? true : false;
            when(expressionEvaluator.evaluate(condition, event)).thenReturn(value);
            count++;
        }

        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                countDownLatch.countDown();
            });
        }
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        Map<String, Object> expectedEventMap = new HashMap<>(getEventMap(testValue));
        expectedEventMap.put(DEFAULT_COUNT_KEY, NUM_THREADS * NUM_EVENTS_PER_BATCH/2);

        rec = (Record<Event>)results.toArray()[0];
        assertThat(expectedEventMap, equalTo(rec.getData().toMap()));
    }

    private List<Record<Event>> getBatchOfEvents(boolean withSameValue) {
        final List<Record<Event>> events = new ArrayList<>();

        for (int i = 0; i < NUM_EVENTS_PER_BATCH; i++) {
            final Map<String, Object> eventMap = (withSameValue) ? getEventMap(testValue) : getEventMap(i % NUM_UNIQUE_EVENTS_PER_BATCH);
            final Event event = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            if (withSameValue) {
                event.put("data", UUID.randomUUID().toString());
            }

            uniqueEventMaps.add(eventMap);
            events.add(new Record<>(event));
        }
        return events;
    }

    private Map<String, Object> getEventMap(int i) {
        final Map<String, Object> eventMap = new HashMap<>();
        eventMap.put("firstRandomNumber", i);
        eventMap.put("secondRandomNumber", i);
        eventMap.put("thirdRandomNumber", i);
        return eventMap;
    }

}
