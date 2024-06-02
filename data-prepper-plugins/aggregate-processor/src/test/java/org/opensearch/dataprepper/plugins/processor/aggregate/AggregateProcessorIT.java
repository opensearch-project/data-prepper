/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.lang3.RandomStringUtils;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RemoveDuplicatesAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PutAllAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PercentSamplerAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PercentSamplerAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.TailSamplerAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.TailSamplerAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterMode;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.OutputFormat;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_COUNT_KEY;
import static org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig.DEFAULT_START_TIME_KEY;

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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.collection.IsIn.in;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.doAnswer;

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
    private static final int ERROR_STATUS = 2;
    @Mock
    private AggregateProcessorConfig aggregateProcessorConfig;

    @Mock
    RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig;
    @Mock
    TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig;

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

        when(aggregateProcessorConfig.getAllowRawEvents()).thenReturn(false);
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(identificationKeys);
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
    }

    private AggregateProcessor createObjectUnderTest() {
        return new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
    }

    @RepeatedTest(value = 2)
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
            when(expressionEvaluator.evaluateConditional(condition, event)).thenReturn(value);
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

    @ParameterizedTest
    @ValueSource(doubles = {5.0, 15.0, 55.0, 92.0, 99.0})
    void aggregateWithPercentSamplerAction(double testPercent) throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        PercentSamplerAggregateActionConfig percentSamplerAggregateActionConfig = new PercentSamplerAggregateActionConfig();
        setField(PercentSamplerAggregateActionConfig.class, percentSamplerAggregateActionConfig, "percent", testPercent);
        aggregateAction = new PercentSamplerAggregateAction(percentSamplerAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();

        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        final CountDownLatch countDownLatch = new CountDownLatch(NUM_THREADS);

        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);
        AtomicInteger allowedEventsCount = new AtomicInteger(0);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.execute(() -> {
                final List<Record<Event>> recordsOut = (List<Record<Event>>) objectUnderTest.doExecute(eventBatch);
                allowedEventsCount.getAndAdd(recordsOut.size());
                countDownLatch.countDown();
            });
        }

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        assertThat((double)allowedEventsCount.get(), closeTo(NUM_THREADS * NUM_EVENTS_PER_BATCH * testPercent/100, 1.0));
    }

    @RepeatedTest(value = 2)
    void aggregateWithRateLimiterAction() throws InterruptedException {
        final int eventsPerSecond = 500;
        lenient().when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(eventsPerSecond);
        lenient().when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.DROP.toString());

        aggregateAction = new RateLimiterAggregateAction(rateLimiterAggregateActionConfig);
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
        // Expect less number of events to be received, because of rate limiting
        assertThat(aggregatedResult.size(), lessThan(NUM_THREADS * NUM_EVENTS_PER_BATCH));
        assertThat(aggregatedResult.size(), lessThanOrEqualTo(eventsPerSecond * GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
    }

    @RepeatedTest(value = 2)
    void aggregateWithRateLimiterActionNoDrops() throws InterruptedException {
        final int eventsPerSecond = 10000;
        lenient().when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(eventsPerSecond);
        aggregateAction = new RateLimiterAggregateAction(rateLimiterAggregateActionConfig);
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

        boolean allThreadsFinished = countDownLatch.await(10L, TimeUnit.SECONDS);

        assertThat(allThreadsFinished, equalTo(true));
        // Expect all events to be received even with rate limiting because no events are dropped
        assertThat(aggregatedResult.size(), equalTo(NUM_THREADS * NUM_EVENTS_PER_BATCH));
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateAction() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
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

        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k,v)));
        assertThat(record.getData().toMap(), hasKey(DEFAULT_START_TIME_KEY));
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateActionWithCondition() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        final String condition = "/firstRandomNumber < 100";
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(condition);
        int count = 0;
        eventBatch = getBatchOfEvents(true);
        for (Record<Event> record: eventBatch) {
            Event event = record.getData();
            boolean value = (count % 2 == 0) ? true : false;
            when(expressionEvaluator.evaluateConditional(condition, event)).thenReturn(value);
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

        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k,v)));
        assertThat(record.getData().toMap(), hasKey(DEFAULT_START_TIME_KEY));
    }

    @RepeatedTest(value = 2)
    void aggregateWithCountAggregateActionWithRawEvents() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        when(aggregateProcessorConfig.getAllowRawEvents()).thenReturn(true);
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
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
                assertThat(recordsOut.size(), equalTo(NUM_EVENTS_PER_BATCH));
                countDownLatch.countDown();
            });
        }
        // wait longer so that the raw events are processed.
        Thread.sleep(2*GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);

        boolean allThreadsFinished = countDownLatch.await(5L, TimeUnit.SECONDS);
        assertThat(allThreadsFinished, equalTo(true));

        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        assertThat(results.size(), equalTo(1));

        Map<String, Object> expectedEventMap = new HashMap<>(getEventMap(testValue));
        expectedEventMap.put(DEFAULT_COUNT_KEY, NUM_THREADS * NUM_EVENTS_PER_BATCH);

        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k,v)));
        assertThat(record.getData().toMap(), hasKey(DEFAULT_START_TIME_KEY));
    }


    @RepeatedTest(value = 2)
    void aggregateWithHistogramAggregateAction() throws InterruptedException, NoSuchFieldException, IllegalAccessException {

        HistogramAggregateActionConfig histogramAggregateActionConfig = new HistogramAggregateActionConfig();
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "outputFormat", OutputFormat.RAW.toString());
        final String testKey = RandomStringUtils.randomAlphabetic(5);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(4)+"_";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testKeyPrefix);
        final String testUnits = RandomStringUtils.randomAlphabetic(3);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", testUnits);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        List<Number> testBuckets = new ArrayList<Number>();
        final double TEST_VALUE_RANGE_MIN = 0.0;
        final double TEST_VALUE_RANGE_MAX = 40.0;
        final double TEST_VALUE_RANGE_STEP = 10.0;
        for (double d = TEST_VALUE_RANGE_MIN; d <= TEST_VALUE_RANGE_MAX; d += TEST_VALUE_RANGE_STEP) {
            testBuckets.add(d);
        }
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", testBuckets);

        aggregateAction = new HistogramAggregateAction(histogramAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        eventBatch = getBatchOfEvents(true);
        for (final Record<Event> record : eventBatch) {
            final double value = ThreadLocalRandom.current().nextDouble(TEST_VALUE_RANGE_MIN-TEST_VALUE_RANGE_STEP, TEST_VALUE_RANGE_MAX+TEST_VALUE_RANGE_STEP);
            Event event = record.getData();
            event.put(testKey, value);
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

        final String expectedCountKey = histogramAggregateActionConfig.getCountKey();
        Map<String, Object> expectedEventMap = new HashMap<>();
        expectedEventMap.put(expectedCountKey, NUM_THREADS * NUM_EVENTS_PER_BATCH);
        final String expectedStartTimeKey = histogramAggregateActionConfig.getStartTimeKey();
        
        final Record<Event> record = (Record<Event>)results.toArray()[0];
        expectedEventMap.forEach((k, v) -> assertThat(record.getData().toMap(), hasEntry(k, v)));
        assertThat(record.getData().toMap(), hasKey(expectedStartTimeKey));
        final String expectedBucketsKey = histogramAggregateActionConfig.getBucketsKey();
        List<Double> bucketsInResult = (ArrayList<Double>)record.getData().toMap().get(expectedBucketsKey);
        for (int i = 0; i < testBuckets.size(); i++) {
            assertThat(testBuckets.get(i).doubleValue(), equalTo(bucketsInResult.get(i)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {20, 40, 60})
    void aggregateWithTailSamplerAction(final int testPercent) throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        final Duration testWaitPeriod = Duration.ofSeconds(3);
        final String testCondition = "/status == "+ERROR_STATUS;
        when(tailSamplerAggregateActionConfig.getPercent()).thenReturn(testPercent);
        when(tailSamplerAggregateActionConfig.getWaitPeriod()).thenReturn(testWaitPeriod);
        when(tailSamplerAggregateActionConfig.getCondition()).thenReturn(testCondition);
        doAnswer(a -> {
            Event event = (Event)a.getArgument(1);
            return event.get("status", Integer.class) == ERROR_STATUS;
        }).when(expressionEvaluator).evaluateConditional(eq(testCondition), any(Event.class));
        aggregateAction = new TailSamplerAggregateAction(tailSamplerAggregateActionConfig, expressionEvaluator);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class))).thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(List.of("traceId"));
        final AggregateProcessor objectUnderTest = createObjectUnderTest();
        final ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        final int numberOfErrorTraces = 2;
        final int numberOfSpans = 5;
        eventBatch = getBatchOfEventsForTailSampling(numberOfErrorTraces, numberOfSpans);
        objectUnderTest.doExecute(eventBatch);
        Thread.sleep(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE * 1000);
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
        List<Event> errorEventList = eventBatch.stream().map(Record::getData).filter(event -> {
            Event ev = ((Event)event);
            return ev.get("status", Integer.class) == ERROR_STATUS;
        }).collect(Collectors.toList());
        Thread.sleep(testWaitPeriod.toMillis()*2);
        Collection<Record<Event>> results = objectUnderTest.doExecute(new ArrayList<Record<Event>>());
        Set<Event> resultsSet =  results.stream().map(Record::getData).collect(Collectors.toSet());
        assertThat(results.size(), greaterThanOrEqualTo(numberOfErrorTraces*numberOfSpans));
        assertThat(results.size(), lessThan(eventBatch.size()*(NUM_THREADS+1)));
        for (final Event event : errorEventList) {
            assertTrue(resultsSet.contains(event));
        }
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

    private List<Record<Event>> getBatchOfEventsForTailSampling(final int numberOfErrorTraces, final int numberOfSpans) {
        final List<Record<Event>> events = new ArrayList<>();
        final int numberOfTraces = numberOfErrorTraces + 10;

        for (int i = 0; i < numberOfTraces; i++) {
            final int status = (i < numberOfErrorTraces) ? ERROR_STATUS : 0;
            for (int j = 0; j < numberOfSpans; j++) {
                final Map<String, Object> eventMap = new HashMap<>();
                eventMap.put("traceId", 10000+i);
                eventMap.put("spanId", j);
                eventMap.put("status", status);
                final Event event = JacksonEvent.builder()
                        .withEventType("event")
                        .withData(eventMap)
                        .build();
                events.add(new Record<>(event));
            }
        }
        return events;
    }

}
