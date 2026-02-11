/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import org.opensearch.dataprepper.plugins.otel.utils.OTelSpanDerivationUtil;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
    void testServerSpansReceiveDerivedAttributes() {
        final Collection<Record<Span>> processedRecords = oTelTraceRawProcessor.doExecute(TEST_TWO_FULL_TRACE_GROUP_RECORDS);

        // We know TEST_TWO_FULL_TRACE_GROUP_RECORDS contains SERVER spans, so let's be specific
        final List<Span> serverSpans = processedRecords.stream()
                .map(Record::getData)
                .filter(span -> "SPAN_KIND_SERVER".equals(span.getKind()))
                .collect(Collectors.toList());

        // Verify we have the expected SERVER spans
        assertThat(serverSpans.size(), equalTo(4)); // 2 traces × 2 SERVER child spans each

        // Test each SERVER span for exact derived attribute values
        for (final Span serverSpan : serverSpans) {
            final Map<String, Object> attributes = serverSpan.getAttributes();

            // Verify all derived attributes are present
            assertTrue(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE),
                      "SERVER span should have derived.fault attribute");
            assertTrue(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE),
                      "SERVER span should have derived.error attribute");
            assertTrue(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE),
                      "SERVER span should have derived.operation attribute");
            assertTrue(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE),
                      "SERVER span should have derived.environment attribute");

            // Test exact expected values based on test data
            final String fault = (String) attributes.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE);
            final String error = (String) attributes.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE);
            final String operation = (String) attributes.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE);
            final String environment = (String) attributes.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE);

            // Test spans have no status and no HTTP attributes, so fault and error should be 0
            assertThat(fault, equalTo("0"));
            assertThat(error, equalTo("0"));

            // Operation should be the span name since no HTTP attributes are present
            assertThat(operation, equalTo(serverSpan.getName()));

            // Environment should be default since no resource attributes are present
            assertThat(environment, equalTo("generic:default"));
        }
    }



    @Test
    void testDerivedAttributesAllScenarios() {
        // Test comprehensive derived attribute scenarios using dedicated JSON test files

        // Scenario 1: ERROR status → fault=1, error=0
        final List<Record<Span>> errorTraceRecords = Arrays.asList(
            new Record<>(buildSpanFromJsonFile("error-status-root-span.json")),
            new Record<>(buildSpanFromJsonFile("error-status-child-span.json"))
        );
        final Collection<Record<Span>> processedErrorRecords = oTelTraceRawProcessor.doExecute(errorTraceRecords);
        final Span errorServerSpan = findServerSpan(processedErrorRecords);

        Map<String, Object> attrs = errorServerSpan.getAttributes();
        assertThat("ERROR status span fault", attrs.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));
        assertThat("ERROR status span error", attrs.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
        assertThat("ERROR status operation", attrs.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("ERROR_OPERATION"));

        // Scenario 2: HTTP 4xx error → fault=0, error=1
        final List<Record<Span>> http4xxTraceRecords = Arrays.asList(
            new Record<>(buildSpanFromJsonFile("http-4xx-root-span.json")),
            new Record<>(buildSpanFromJsonFile("http-4xx-child-span.json"))
        );
        final Collection<Record<Span>> processedHttp4xxRecords = oTelTraceRawProcessor.doExecute(http4xxTraceRecords);
        final Span http4xxServerSpan = findServerSpan(processedHttp4xxRecords);

        attrs = http4xxServerSpan.getAttributes();
        assertThat("HTTP 4xx span fault", attrs.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("0"));
        assertThat("HTTP 4xx span error", attrs.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("1"));
        assertThat("HTTP 4xx operation (preserves span name)", attrs.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("USER_LOOKUP"));

        // Scenario 3: HTTP 5xx error → fault=1, error=0
        final List<Record<Span>> http5xxTraceRecords = Arrays.asList(
            new Record<>(buildSpanFromJsonFile("http-5xx-root-span.json")),
            new Record<>(buildSpanFromJsonFile("http-5xx-child-span.json"))
        );
        final Collection<Record<Span>> processedHttp5xxRecords = oTelTraceRawProcessor.doExecute(http5xxTraceRecords);
        final Span http5xxServerSpan = findServerSpan(processedHttp5xxRecords);

        attrs = http5xxServerSpan.getAttributes();
        assertThat("HTTP 5xx span fault", attrs.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("1"));
        assertThat("HTTP 5xx span error", attrs.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
        assertThat("HTTP 5xx operation (preserves span name)", attrs.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("ORDER_CREATE"));

        // Scenario 4: Custom environment → production environment
        final List<Record<Span>> customEnvTraceRecords = Arrays.asList(
            new Record<>(buildSpanFromJsonFile("custom-env-root-span.json")),
            new Record<>(buildSpanFromJsonFile("custom-env-child-span.json"))
        );
        final Collection<Record<Span>> processedEnvRecords = oTelTraceRawProcessor.doExecute(customEnvTraceRecords);
        final Span envServerSpan = findServerSpan(processedEnvRecords);

        attrs = envServerSpan.getAttributes();
        assertThat("Custom environment", attrs.get(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), equalTo("production"));
        assertThat("Custom env operation", attrs.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("HEALTH_CHECK"));

        // Scenario 5: Generic span name → HTTP operation derivation
        final List<Record<Span>> genericNameTraceRecords = Arrays.asList(
            new Record<>(buildSpanFromJsonFile("generic-name-root-span.json")),
            new Record<>(buildSpanFromJsonFile("generic-name-child-span.json"))
        );
        final Collection<Record<Span>> processedGenericRecords = oTelTraceRawProcessor.doExecute(genericNameTraceRecords);
        final Span genericServerSpan = findServerSpan(processedGenericRecords);

        attrs = genericServerSpan.getAttributes();
        assertThat("Generic span name HTTP derivation", attrs.get(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), equalTo("GET /api"));
        assertThat("Generic span fault", attrs.get(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), equalTo("0"));
        assertThat("Generic span error", attrs.get(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), equalTo("0"));
    }

    private Span findServerSpan(Collection<Record<Span>> records) {
        return records.stream()
                .map(Record::getData)
                .filter(span -> "SPAN_KIND_SERVER".equals(span.getKind()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No SERVER span found in processed records"));
    }

    @Test
    void testNonServerSpansDoNotReceiveDerivedAttributes() {
        final Collection<Record<Span>> processedRecords = oTelTraceRawProcessor.doExecute(TEST_TWO_FULL_TRACE_GROUP_RECORDS);

        // Verify that non-SERVER spans do not have derived attributes
        for (Record<Span> record : processedRecords) {
            final Span span = record.getData();
            if (!"SPAN_KIND_SERVER".equals(span.getKind())) {
                final Map<String, Object> attributes = span.getAttributes();
                
                assertFalse(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_FAULT_ATTRIBUTE), 
                           "Non-SERVER span should not have derived.fault attribute");
                assertFalse(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_ERROR_ATTRIBUTE), 
                           "Non-SERVER span should not have derived.error attribute");  
                assertFalse(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_OPERATION_ATTRIBUTE), 
                           "Non-SERVER span should not have derived.operation attribute");
                assertFalse(attributes.containsKey(OTelSpanDerivationUtil.DERIVED_ENVIRONMENT_ATTRIBUTE), 
                           "Non-SERVER span should not have derived.environment attribute");
            }
        }
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
            "0, 4",
            "2, 6"
    })
    void traceGroupCacheMaxSize_provides_an_upper_bound(final long cacheMaxSize, final int leastExpectedProcessedRecordCount) {
        reset(config);
        when(config.getTraceFlushIntervalSeconds()).thenReturn(TEST_TRACE_FLUSH_INTERVAL);
        when(config.getTraceGroupCacheMaxSize()).thenReturn(cacheMaxSize);
        when(config.getTraceGroupCacheTimeToLive()).thenReturn(OtelTraceRawProcessorConfig.DEFAULT_TRACE_ID_TTL);

        oTelTraceRawProcessor = new OTelTraceRawProcessor(config, pipelineDescription, pluginMetrics);

        final Collection<Record<Span>> processedRecords = new ArrayList<>();
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS));
        processedRecords.addAll(oTelTraceRawProcessor.doExecute(TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS));

        // Caffeine cache eviction is not a synchronized action so trying to make an exact match with the
        // expected record count may not always match depending on when the records are getting evicted.
        // So changing this assertion from equals to greater than equals to expected record count
        assertTrue(processedRecords.size() >= leastExpectedProcessedRecordCount,
                String.format("Processed records of %d should be at least the expected record count of %d",
                        processedRecords.size(), leastExpectedProcessedRecordCount));
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
            final Map<String, Object> status = (Map<String, Object>) spanMap.get("status");
            final Map<String, Object> attributes = (Map<String, Object>) spanMap.get("attributes");
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
            if (status != null) {
                spanBuilder = spanBuilder.withStatus(status);
            }
            if (attributes != null) {
                spanBuilder = spanBuilder.withAttributes(attributes);
            }
            DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
            if (parentSpanId == null || parentSpanId.isEmpty()) {
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
