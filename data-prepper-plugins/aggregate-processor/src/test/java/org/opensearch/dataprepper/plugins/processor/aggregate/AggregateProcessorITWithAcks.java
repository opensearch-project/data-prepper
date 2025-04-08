/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.event.AggregateEventHandle;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelper;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.core.pipeline.common.FutureHelperResult;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSet;
import org.opensearch.dataprepper.core.acknowledgements.DefaultAcknowledgementSetMetrics;
import org.opensearch.dataprepper.core.pipeline.ProcessWorker;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.AppendAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.AppendAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.CountAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.HistogramAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterMode;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RateLimiterAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PercentSamplerAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PercentSamplerAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.TailSamplerAggregateActionConfig;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.TailSamplerAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.RemoveDuplicatesAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.PutAllAggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.actions.OutputFormat;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;


import org.apache.commons.lang3.RandomStringUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;


public class AggregateProcessorITWithAcks {
    private static final int testValue = 1;
    private static final int GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE = 2;
    private static final int NUM_UNIQUE_EVENTS_PER_BATCH = 8;
    private static final int NUM_EVENTS_PER_BATCH = 5;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(30);

    @Mock
    private Pipeline pipeline;
    @Mock
    private Buffer buffer;
    @Mock
    private Source source;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private CheckpointState checkpointState;
    @Mock
    private PluginModel actionConfiguration;
    @Mock
    private AggregateProcessorConfig aggregateProcessorConfig;
    private int callCount;
    private boolean aggregatedResultReceived;
    List<Record<Event>> records;
    private String testKey;

    private PluginMetrics pluginMetrics;
    private List<Processor> processors;
    private List<Future<Void>> sinkFutures;
    AcknowledgementSet acknowledgementSet;
    ScheduledExecutorService scheduledExecutorService;
    List<Record<Event>> aggregatedResults;

    @BeforeEach
    void setup() {
        testKey = UUID.randomUUID().toString();
        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        scheduledExecutorService = Executors.newScheduledThreadPool(3);
        acknowledgementSet = new DefaultAcknowledgementSet(scheduledExecutorService, (result) -> {}, Duration.ofSeconds(10), new DefaultAcknowledgementSetMetrics(pluginMetrics));
        final List<String> identificationKeys = new ArrayList<>();
        identificationKeys.add("firstRandomNumber");
        identificationKeys.add("secondRandomNumber");
        identificationKeys.add("thirdRandomNumber");
        callCount = 0;
        aggregatedResultReceived = false;
        aggregatedResults = new ArrayList<>();

        pipeline = mock(Pipeline.class);
        source = mock(Source.class);
        buffer = mock(Buffer.class);
        processors = List.of();
        aggregateProcessorConfig = mock(AggregateProcessorConfig.class);
        actionConfiguration = mock(PluginModel.class);
        pluginFactory = mock(PluginFactory.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(pipeline.isStopRequested()).thenReturn(false).thenReturn(true);
        when(source.areAcknowledgementsEnabled()).thenReturn(true);
        when(pipeline.getSource()).thenReturn(source);
        when(pipeline.getBuffer()).thenReturn(buffer);
        when(buffer.isEmpty()).thenReturn(true);
        when(pipeline.getPeerForwarderDrainTimeout()).thenReturn(Duration.ofMillis(100));
        when(pipeline.getReadBatchTimeoutInMillis()).thenReturn(500);
        when(aggregateProcessorConfig.getOutputUnaggregatedEvents()).thenReturn(false);
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(identificationKeys);
        when(aggregateProcessorConfig.getWhenCondition()).thenReturn(null);

        records = getRecords(testKey, testValue, acknowledgementSet);
        acknowledgementSet.complete();
        checkpointState = mock(CheckpointState.class);
        final Map.Entry<Collection, CheckpointState> readResult = Map.entry(records, checkpointState);
        
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenAnswer(a -> {
            if (callCount == 0) {
                callCount++;
                return Map.entry(records, checkpointState);
            } else {
                return Map.entry(List.of(), checkpointState);
            }
        });

        final Future<Void> sinkFuture = mock(Future.class);
        sinkFutures = List.of(sinkFuture);
        doAnswer( a -> {
            List<Record<Event>> receivedRecords = (List<Record<Event>>)a.getArgument(0);        
            if (receivedRecords.size() > 0) {
                aggregatedResults = receivedRecords;
                for (Record<Event> record: receivedRecords) {
                    if (record.getData().getEventHandle() instanceof AggregateEventHandle) {
                        aggregatedResultReceived = true;
                    }
                    record.getData().getEventHandle().release(true);
                }
            }
            
            return sinkFutures;
        }).when(pipeline).publishToSinks(any());
        when(aggregateProcessorConfig.getAggregateAction()).thenReturn(actionConfiguration);
        when(actionConfiguration.getPluginName()).thenReturn(UUID.randomUUID().toString());
        when(actionConfiguration.getPluginSettings()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testHistogramAggregation() throws Exception {
        HistogramAggregateActionConfig histogramAggregateActionConfig = new HistogramAggregateActionConfig();
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "outputFormat", OutputFormat.RAW);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "key", testKey);
        final String testKeyPrefix = RandomStringUtils.randomAlphabetic(4)+"_";
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "generatedKeyPrefix", testKeyPrefix);
        final String testUnits = RandomStringUtils.randomAlphabetic(3);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "units", testUnits);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "recordMinMax", true);
        List<Number> testBuckets = new ArrayList<Number>();
        testBuckets.add(10.0);
        setField(HistogramAggregateActionConfig.class, histogramAggregateActionConfig, "buckets", testBuckets);
        AggregateAction aggregateAction = new HistogramAggregateAction(histogramAggregateActionConfig);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofMillis(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);

            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertTrue(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }

    @Test
    public void testPercentSamplerAggregation() throws Exception {
        double testPercent = 50.0;
        PercentSamplerAggregateActionConfig percentSamplerAggregateActionConfig = new PercentSamplerAggregateActionConfig();
        setField(PercentSamplerAggregateActionConfig.class, percentSamplerAggregateActionConfig, "percent", testPercent);
        AggregateAction aggregateAction = new PercentSamplerAggregateAction(percentSamplerAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));

        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);

            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), greaterThanOrEqualTo(1));
                assertThat(aggregatedResults.size(), lessThan(5));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }


    @Test
    public void testPutAllAggregation() throws Exception {
        AggregateAction aggregateAction = new PutAllAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);

            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertTrue(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }


    @Test
    public void testRateLimiterDropAggregation() throws Exception {
        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = mock(RateLimiterAggregateActionConfig.class);
        final int eventsPerSecond = 1;
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(eventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.DROP);
        AggregateAction aggregateAction = new RateLimiterAggregateAction(rateLimiterAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);


            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }

    @Test
    public void testRemoveDuplicatesAggregation() {
        AggregateAction aggregateAction = new RemoveDuplicatesAggregateAction();
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }

    @Test
    public void testRateLimiterNoDropAggregation() throws Exception {
        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = mock(RateLimiterAggregateActionConfig.class);
        final int eventsPerSecond = 50;
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(eventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.BLOCK);
        AggregateAction aggregateAction = new RateLimiterAggregateAction(rateLimiterAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);

            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(5));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }


    @Test
    public void testRateLimiterNoDropAggregationWithMultipleAcknowledgementSets() throws Exception {
        AcknowledgementSet acknowledgementSet2 = new DefaultAcknowledgementSet(scheduledExecutorService, (result) -> {}, Duration.ofSeconds(10), new DefaultAcknowledgementSetMetrics(pluginMetrics));
        AcknowledgementSet acknowledgementSet3 = new DefaultAcknowledgementSet(scheduledExecutorService, (result) -> {}, Duration.ofSeconds(10), new DefaultAcknowledgementSetMetrics(pluginMetrics));
        final List<Record<Event>> records2 = getRecords(testKey, 1, acknowledgementSet2);
        acknowledgementSet2.complete();
        final List<Record<Event>> records3 = getRecords(testKey, 1, acknowledgementSet3);
        acknowledgementSet3.complete();
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(List.of(testKey));

        RateLimiterAggregateActionConfig rateLimiterAggregateActionConfig = mock(RateLimiterAggregateActionConfig.class);
        final int eventsPerSecond = 50;
        when(rateLimiterAggregateActionConfig.getEventsPerSecond()).thenReturn(eventsPerSecond);
        when(rateLimiterAggregateActionConfig.getWhenExceeds()).thenReturn(RateLimiterMode.BLOCK);
        AggregateAction aggregateAction = new RateLimiterAggregateAction(rateLimiterAggregateActionConfig);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        buffer = mock(Buffer.class);
        when(buffer.isEmpty()).thenReturn(true);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenAnswer(a -> {
            if (callCount == 0) {
                callCount++;
                return Map.entry(records, checkpointState);
            } else if (callCount == 1) {
                callCount++;
                return Map.entry(records2, checkpointState);
            } else if (callCount == 2) {
                callCount++;
                return Map.entry(records3, checkpointState);
            } else {
                return Map.entry(List.of(), checkpointState);
            }
        });

        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(5));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }


    @Test
    public void testCountAggregationWithMultipleAcknowledgementSets() throws Exception {
        AcknowledgementSet acknowledgementSet2 = new DefaultAcknowledgementSet(scheduledExecutorService, (result) -> {}, Duration.ofSeconds(10), new DefaultAcknowledgementSetMetrics(pluginMetrics));
        AcknowledgementSet acknowledgementSet3 = new DefaultAcknowledgementSet(scheduledExecutorService, (result) -> {}, Duration.ofSeconds(10), new DefaultAcknowledgementSetMetrics(pluginMetrics));
        final List<Record<Event>> records2 = getRecords(testKey, 1, acknowledgementSet2);
        acknowledgementSet2.complete();
        final List<Record<Event>> records3 = getRecords(testKey, 1, acknowledgementSet3);
        acknowledgementSet3.complete();
        when(aggregateProcessorConfig.getIdentificationKeys()).thenReturn(List.of(testKey));

        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW);
        AggregateAction aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        callCount = 0;
        buffer = mock(Buffer.class);
        when(buffer.isEmpty()).thenReturn(true);
        when(buffer.read(pipeline.getReadBatchTimeoutInMillis())).thenAnswer(a -> {
            if (callCount == 0) {
                callCount++;
                return Map.entry(records, checkpointState);
            } else if (callCount == 1) {
                callCount++;
                return Map.entry(records2, checkpointState);
            } else if (callCount == 2) {
                callCount++;
                return Map.entry(records3, checkpointState);
            } else {
                return Map.entry(List.of(), checkpointState);
            }
        });
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertTrue(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet2).isDone());
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet3).isDone());
            });
    }

    @Test
    public void testCountAggregation() throws Exception {
        CountAggregateActionConfig countAggregateActionConfig = new CountAggregateActionConfig();
        setField(CountAggregateActionConfig.class, countAggregateActionConfig, "outputFormat", OutputFormat.RAW);
        AggregateAction aggregateAction = new CountAggregateAction(countAggregateActionConfig);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofMillis(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class)))
                .thenReturn(aggregateAction);
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertTrue(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }
 
    @Test
    public void testTailSamplerAggregationWithNoErrors() throws Exception {
        TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig = mock(TailSamplerAggregateActionConfig.class);
        final Duration testWaitPeriod = Duration.ofMillis(1);
        final String testCondition = "/status == 2";
        when(tailSamplerAggregateActionConfig.getPercent()).thenReturn(100);
        when(tailSamplerAggregateActionConfig.getWaitPeriod()).thenReturn(testWaitPeriod);
        when(tailSamplerAggregateActionConfig.getCondition()).thenReturn(testCondition);
        when(expressionEvaluator.evaluateConditional(eq(testCondition), any(Event.class))).thenReturn(false);
        AggregateAction aggregateAction = new TailSamplerAggregateAction(tailSamplerAggregateActionConfig, expressionEvaluator);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class))).thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);


            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(5));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }


    
    @Test
    public void testTailSamplerAggregation() throws Exception {
        TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig = mock(TailSamplerAggregateActionConfig.class);
        final Duration testWaitPeriod = Duration.ofMillis(1);
        final String testCondition = "/status == 2";
        when(tailSamplerAggregateActionConfig.getPercent()).thenReturn(50);
        when(tailSamplerAggregateActionConfig.getWaitPeriod()).thenReturn(testWaitPeriod);
        when(tailSamplerAggregateActionConfig.getCondition()).thenReturn(testCondition);
        when(expressionEvaluator.evaluateConditional(eq(testCondition), any(Event.class))).thenReturn(true);
        AggregateAction aggregateAction = new TailSamplerAggregateAction(tailSamplerAggregateActionConfig, expressionEvaluator);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class))).thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertFalse(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(5));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }

    @Test
    public void testAppendAggregation() throws Exception {
        AppendAggregateActionConfig appendAggregateActionConfig = mock(AppendAggregateActionConfig.class);
        when(appendAggregateActionConfig.getKeysToAppend()).thenReturn(List.of(testKey));
        AggregateAction aggregateAction = new AppendAggregateAction(appendAggregateActionConfig);
        when(pluginFactory.loadPlugin(eq(AggregateAction.class), any(PluginSetting.class))).thenReturn(aggregateAction);
        when(aggregateProcessorConfig.getGroupDuration()).thenReturn(Duration.ofSeconds(GROUP_DURATION_FOR_ONLY_SINGLE_CONCLUDE));
        final Processor processor = new AggregateProcessor(aggregateProcessorConfig, pluginMetrics, pluginFactory, expressionEvaluator);
        processors = List.of(processor);
        when(pipeline.getProcessors()).thenReturn(processors);
        final FutureHelperResult<Void> futureHelperResult = mock(FutureHelperResult.class);
        when(futureHelperResult.getFailedReasons()).thenReturn(Collections.emptyList());
        try (final MockedStatic<FutureHelper> futureHelperMockedStatic = mockStatic(FutureHelper.class)) {
            futureHelperMockedStatic.when(() -> FutureHelper.awaitFuturesIndefinitely(sinkFutures))
                    .thenReturn(futureHelperResult);

            final ProcessWorker processWorker = new ProcessWorker(buffer, processors, pipeline);
            processWorker.run();
        }
        await().atMost(TEST_TIMEOUT)
            .untilAsserted(() -> {
                assertTrue(aggregatedResultReceived);
                assertThat(aggregatedResults.size(), equalTo(1));
                assertTrue(((DefaultAcknowledgementSet)acknowledgementSet).isDone());
            });
    }
    

    private List<Record<Event>> getRecords(String key, int value, AcknowledgementSet ackSet) {
        final List<Record<Event>> events = new ArrayList<>();
        final Map<String, Object> eventMap = Map.of(key, value);

        for (int i = 0; i < NUM_EVENTS_PER_BATCH; i++) {
            final Event event = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(eventMap)
                    .build();
            events.add(new Record<>(event));
            ackSet.add(event);
        }
        return events;
    }

}

