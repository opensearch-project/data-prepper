/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.TraceGroupFields;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OTelTraceRawProcessorTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long TEST_TRACE_FLUSH_INTERVAL = 3L;
    private static final int TEST_CONCURRENCY_SCALE = 2;

    private static final String TEST_TRACE_GROUP_1_ROOT_SPAN_JSON_FILE = "trace-group-1-root-span.json";
    private static final String TEST_TRACE_GROUP_1_CHILD_SPAN_1_JSON_FILE = "trace-group-1-child-span-1.json";
    private static final String TEST_TRACE_GROUP_1_CHILD_SPAN_2_JSON_FILE = "trace-group-1-child-span-2.json";
    private static final String TEST_TRACE_GROUP_2_ROOT_SPAN_JSON_FILE = "trace-group-2-root-span.json";
    private static final String TEST_TRACE_GROUP_2_CHILD_SPAN_1_JSON_FILE = "trace-group-2-child-span-1.json";
    private static final String TEST_TRACE_GROUP_2_CHILD_SPAN_2_JSON_FILE = "trace-group-2-child-span-2.json";

    private Span TEST_TRACE_GROUP_1_ROOT_SPAN;
    private Span TEST_TRACE_GROUP_1_CHILD_SPAN_1;
    private Span TEST_TRACE_GROUP_1_CHILD_SPAN_2;
    private Span TEST_TRACE_GROUP_2_ROOT_SPAN;
    private Span TEST_TRACE_GROUP_2_CHILD_SPAN_1;
    private Span TEST_TRACE_GROUP_2_CHILD_SPAN_2;

    private List<Record<Span>> TEST_ONE_FULL_TRACE_GROUP_RECORDS;
    private List<Record<Span>> TEST_ONE_TRACE_GROUP_MISSING_ROOT_RECORDS;
    private List<Record<Span>> TEST_TWO_FULL_TRACE_GROUP_RECORDS;
    private List<Record<Span>> TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS;
    private List<Record<Span>> TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS;
    private List<Record<Span>> TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS;

    private OtelTraceRawProcessorConfig config;
    private PluginMetrics pluginMetrics;
    public OTelTraceRawProcessor oTelTraceRawProcessor;
    public ExecutorService executorService;
    private PipelineDescription pipelineDescription;

    @BeforeEach
    void setup() {
        TEST_TRACE_GROUP_1_ROOT_SPAN = buildSpanFromJsonFile(TEST_TRACE_GROUP_1_ROOT_SPAN_JSON_FILE);
        TEST_TRACE_GROUP_1_CHILD_SPAN_1 = buildSpanFromJsonFile(TEST_TRACE_GROUP_1_CHILD_SPAN_1_JSON_FILE);
        TEST_TRACE_GROUP_1_CHILD_SPAN_2 = buildSpanFromJsonFile(TEST_TRACE_GROUP_1_CHILD_SPAN_2_JSON_FILE);
        TEST_TRACE_GROUP_2_ROOT_SPAN = buildSpanFromJsonFile(TEST_TRACE_GROUP_2_ROOT_SPAN_JSON_FILE);
        TEST_TRACE_GROUP_2_CHILD_SPAN_1 = buildSpanFromJsonFile(TEST_TRACE_GROUP_2_CHILD_SPAN_1_JSON_FILE);
        TEST_TRACE_GROUP_2_CHILD_SPAN_2 = buildSpanFromJsonFile(TEST_TRACE_GROUP_2_CHILD_SPAN_2_JSON_FILE);
        TEST_ONE_FULL_TRACE_GROUP_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());
        TEST_ONE_TRACE_GROUP_MISSING_ROOT_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());
        TEST_TWO_FULL_TRACE_GROUP_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2,
                        TEST_TRACE_GROUP_2_ROOT_SPAN, TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());
        TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());
        TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_2_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());
        TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS = Stream.of(
                        TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2,
                        TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
                .map(Record::new).collect(Collectors.toList());

        config = mock(OtelTraceRawProcessorConfig.class);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getNumberOfProcessWorkers()).thenReturn(TEST_CONCURRENCY_SCALE);
        pluginMetrics = mock(PluginMetrics.class);

        when(config.getTraceFlushIntervalSeconds()).thenReturn(TEST_TRACE_FLUSH_INTERVAL);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL);

        oTelTraceRawProcessor = new OTelTraceRawProcessor(config, pipelineDescription, pluginMetrics);
        executorService = Executors.newFixedThreadPool(TEST_CONCURRENCY_SCALE);
    }

    @AfterEach
    void tearDown() {
        oTelTraceRawProcessor.shutdown();
        executorService.shutdown();
    }

    @Test
    void testEmptyCollection() {
        assertThat(oTelTraceRawProcessor.doExecute(Collections.EMPTY_LIST)).isEmpty();
    }

    @Test
    void testExportRequestFlushByParentSpan() {
        final Collection<Record<Span>> processedRecords = oTelTraceRawProcessor.doExecute(TEST_TWO_FULL_TRACE_GROUP_RECORDS);

        Assertions.assertThat(processedRecords.size()).isEqualTo(6);
        Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(0);
    }

    @Test
    void testExportRequestFlushByParentSpanMultiThread() throws InterruptedException, ExecutionException {
        final List<Record<Span>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();
        futures.addAll(submitRecords(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS));
        futures.addAll(submitRecords(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS));
        for (Future<Collection<Record<Span>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<Span>>>> futureList = submitRecords(Collections.emptyList());
            for (Future<Collection<Record<Span>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(6);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(0);
        });
    }

    @Test
    void testExportRequestFlushByGC() {
        oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS);
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<Span>> processedRecords = (List<Record<Span>>) oTelTraceRawProcessor.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(4);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(4);
        });
    }

    @Test
    void testExportRequestFlushByMixedMultiThread() throws InterruptedException, ExecutionException {
        List<Record<Span>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();
        futures.addAll(submitRecords(TEST_ONE_FULL_TRACE_GROUP_RECORDS));
        futures.addAll(submitRecords(TEST_ONE_TRACE_GROUP_MISSING_ROOT_RECORDS));
        for (Future<Collection<Record<Span>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<Span>>>> futureList = submitRecords(Collections.emptyList());
            for (Future<Collection<Record<Span>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(5);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(2);
        });
    }

    @Test
    void testPrepareForShutdown() {
        // Assert no records in memory
        assertTrue(oTelTraceRawProcessor.isReadyForShutdown());

        // Add records to memory/queue
        oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS);

        // Assert records exist in memory
        assertFalse(oTelTraceRawProcessor.isReadyForShutdown());

        // Force records to be flushed
        oTelTraceRawProcessor.prepareForShutdown();
        oTelTraceRawProcessor.doExecute(Collections.emptyList());

        // Assert records have been flushed
        assertTrue(oTelTraceRawProcessor.isReadyForShutdown());
    }

    @Test
    void testGetIdentificationKeys() {
        final Collection<String> expectedIdentificationKeys = oTelTraceRawProcessor.getIdentificationKeys();
        assertThat(expectedIdentificationKeys, equalTo(Collections.singleton("traceId")));
    }

    @Test
    void testMetricsOnTraceGroup() {
        ArgumentCaptor<Object> gaugeObjectArgumentCaptor = ArgumentCaptor.forClass(Object.class);
        ArgumentCaptor<ToDoubleFunction> gaugeFunctionArgumentCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(OTelTraceRawProcessor.TRACE_GROUP_CACHE_COUNT_METRIC_NAME), gaugeObjectArgumentCaptor.capture(), gaugeFunctionArgumentCaptor.capture());
        final Object actualMeasuredObject = gaugeObjectArgumentCaptor.getValue();
        final ToDoubleFunction actualFunction = gaugeFunctionArgumentCaptor.getValue();

        assertThat(actualFunction.applyAsDouble(actualMeasuredObject), equalTo(0.0));

        oTelTraceRawProcessor.doExecute(TEST_TWO_FULL_TRACE_GROUP_RECORDS);

        assertThat(actualFunction.applyAsDouble(actualMeasuredObject), equalTo(2.0));
    }

    @Test
    void testMetricsOnSpanSet() {
        ArgumentCaptor<Object> gaugeObjectArgumentCaptor = ArgumentCaptor.forClass(Object.class);
        ArgumentCaptor<ToDoubleFunction> gaugeFunctionArgumentCaptor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(OTelTraceRawProcessor.SPAN_SET_COUNT_METRIC_NAME), gaugeObjectArgumentCaptor.capture(), gaugeFunctionArgumentCaptor.capture());
        final Object actualMeasuredObject = gaugeObjectArgumentCaptor.getValue();
        final ToDoubleFunction actualFunction = gaugeFunctionArgumentCaptor.getValue();

        assertThat(actualFunction.applyAsDouble(actualMeasuredObject), equalTo(0.0));

        oTelTraceRawProcessor.doExecute(TEST_ONE_TRACE_GROUP_MISSING_ROOT_RECORDS);

        assertThat(actualFunction.applyAsDouble(actualMeasuredObject), equalTo(1.0));
    }

    @ParameterizedTest
    @CsvSource({
            "1, 4",
            "2, 6"
    })
    void traceGroupCacheMaxSize_provides_an_upper_bound(final long cacheMaxSize, final int expectedProcessedRecords) {
        reset(config);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(TEST_TRACE_FLUSH_INTERVAL);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(cacheMaxSize);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL);

        oTelTraceRawProcessor = new OTelTraceRawProcessor(config, pipelineDescription, pluginMetrics);

        final Collection<Record<Span>> processedRecords = new ArrayList<>();
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS));
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS));

        MatcherAssert.assertThat(processedRecords.size(), equalTo(expectedProcessedRecords));
        MatcherAssert.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords), equalTo(0));
    }

    @ParameterizedTest
    @CsvSource({
            "100, 4",
            "2000, 6"
    })
    void traceGroupCacheTimeToLive_causes_trace_group_expiry(final long traceIdTtlMillis, final int expectedProcessedRecords) throws InterruptedException {
        reset(config);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(TEST_TRACE_FLUSH_INTERVAL);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(OtelTraceRawProcessorConfig.MAX_TRACE_ID_CACHE_SIZE);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(Duration.ofMillis(traceIdTtlMillis));

        oTelTraceRawProcessor = new OTelTraceRawProcessor(config, pipelineDescription, pluginMetrics);

        final Collection<Record<Span>> processedRecords = new ArrayList<>();
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS));
        Thread.sleep(200);
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS));

        MatcherAssert.assertThat(processedRecords.size(), equalTo(expectedProcessedRecords));
        MatcherAssert.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords), equalTo(0));
    }

    private static Span buildSpanFromJsonFile(final String jsonFileName) {
        JacksonSpan.Builder spanBuilder = JacksonSpan.builder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelTraceRawProcessorTest.class.getClassLoader().getResourceAsStream(jsonFileName))){
            final Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
            final String traceId = (String) spanMap.get("traceId");
            final String spanId = (String) spanMap.get("spanId");
            final String parentSpanId = (String) spanMap.get("parentSpanId");
            final String traceState = (String) spanMap.get("traceState");
            final String name = (String) spanMap.get("name");
            final String kind = (String) spanMap.get("kind");
            final Long durationInNanos = ((Number) spanMap.get("durationInNanos")).longValue();
            final String startTime = (String) spanMap.get("startTime");
            final String endTime = (String) spanMap.get("endTime");
            spanBuilder = spanBuilder
                    .withTraceId(traceId)
                    .withSpanId(spanId)
                    .withParentSpanId(parentSpanId)
                    .withTraceState(traceState)
                    .withName(name)
                    .withKind(kind)
                    .withDurationInNanos(durationInNanos)
                    .withStartTime(startTime)
                    .withEndTime(endTime)
                    .withTraceGroup(null);
            DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
            if (parentSpanId.isEmpty()) {
                final Integer statusCode = (Integer) ((Map<String, Object>) spanMap.get("traceGroupFields")).get("statusCode");
                traceGroupFieldsBuilder = traceGroupFieldsBuilder
                        .withStatusCode(statusCode)
                        .withEndTime(endTime)
                        .withDurationInNanos(durationInNanos);
                final String traceGroup = (String) spanMap.get("traceGroup");
                spanBuilder = spanBuilder.withTraceGroup(traceGroup);
            }
            spanBuilder.withTraceGroupFields(traceGroupFieldsBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return spanBuilder.build();
    }

    private List<Future<Collection<Record<Span>>>> submitRecords(Collection<Record<Span>> records) {
        final List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> oTelTraceRawProcessor.doExecute(records)));
        return futures;
    }

    private int getMissingTraceGroupFieldsSpanCount(final Collection<Record<Span>> records) {
        int count = 0;
        for (Record<Span> record: records) {
            final Span span = record.getData();
            final String traceGroup = span.getTraceGroup();
            final TraceGroupFields traceGroupFields = span.getTraceGroupFields();
            if (Stream.of(
                    traceGroup,
                    traceGroupFields.getEndTime(),
                    traceGroupFields.getDurationInNanos(),
                    traceGroupFields.getStatusCode()).allMatch(Objects::isNull)) {
                count += 1;
            }
        }
        return count;
    }
}

