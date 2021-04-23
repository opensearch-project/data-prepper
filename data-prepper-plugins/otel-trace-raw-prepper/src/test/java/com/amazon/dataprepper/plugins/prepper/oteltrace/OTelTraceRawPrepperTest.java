package com.amazon.dataprepper.plugins.prepper.oteltrace;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.micrometer.core.instrument.Measurement;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class OTelTraceRawPrepperTest {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long TEST_TRACE_FLUSH_INTERVAL = 3L;
    private static final long TEST_ROOT_SPAN_FLUSH_DELAY = 1L;
    private static final int TEST_CONCURRENCY_SCALE = 2;

    private static final String TEST_REQUEST_ONE_FULL_TRACE_GROUP_JSON_FILE = "sample-request-one-full-trace-group.json";
    private static final String TEST_REQUEST_ONE_TRACE_GROUP_MISSING_ROOT_JSON_FILE = "sample-request-one-trace-group-missing-root.json";
    private static final String TEST_REQUEST_TWO_FULL_TRACE_GROUP_JSON_FILE = "sample-request-two-full-trace-group.json";
    private static final String TEST_REQUEST_TWO_TRACE_GROUP_INTERLEAVED_JSON_FILE_1 = "sample-request-two-trace-group-interleaved-1.json";
    private static final String TEST_REQUEST_TWO_TRACE_GROUP_INTERLEAVED_JSON_FILE_2 = "sample-request-two-trace-group-interleaved-2.json";
    private static final String TEST_REQUEST_TWO_TRACE_GROUP_MISSING_ROOTS_JSON_FILE = "sample-request-two-trace-group-missing-roots.json";

    PluginSetting pluginSetting;
    public OTelTraceRawPrepper oTelTraceRawPrepper;
    public ExecutorService executorService;

    @Before
    public void setup() {
        MetricsTestUtil.initMetrics();
        pluginSetting = new PluginSetting(
                "OTelTrace",
                new HashMap<String, Object>() {{
                    put(OtelTraceRawPrepperConfig.TRACE_FLUSH_INTERVAL, TEST_TRACE_FLUSH_INTERVAL);
                    put(OtelTraceRawPrepperConfig.ROOT_SPAN_FLUSH_DELAY, TEST_ROOT_SPAN_FLUSH_DELAY);
                }});
        pluginSetting.setPipelineName("pipelineOTelTrace");
        pluginSetting.setProcessWorkers(TEST_CONCURRENCY_SCALE);
        oTelTraceRawPrepper = new OTelTraceRawPrepper(pluginSetting);
        executorService = Executors.newFixedThreadPool(TEST_CONCURRENCY_SCALE);
    }

    @After
    public void tearDown() {
        oTelTraceRawPrepper.shutdown();
        executorService.shutdown();
    }

    @Test
    public void testResourceSpansProcessingErrorMetrics() {
        ExportTraceServiceRequest mockData = mock(ExportTraceServiceRequest.class);
        Record record = new Record(mockData);
        ResourceSpans mockResourceSpans = mock(ResourceSpans.class);
        List<ResourceSpans> mockResourceSpansList = Collections.singletonList(mockResourceSpans);

        when(mockData.getResourceSpansList()).thenReturn(mockResourceSpansList);
        when(mockResourceSpans.getResource()).thenThrow(new RuntimeException());

        oTelTraceRawPrepper.doExecute(Collections.singletonList(record));

        final List<Measurement> resourceSpansErrorsMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("pipelineOTelTrace").add("OTelTrace")
                        .add(OTelTraceRawPrepper.RESOURCE_SPANS_PROCESSING_ERRORS).toString());
        final List<Measurement> totalErrorsMeasurement = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add("pipelineOTelTrace").add("OTelTrace")
                        .add(OTelTraceRawPrepper.TOTAL_PROCESSING_ERRORS).toString());

        Assert.assertEquals(1, resourceSpansErrorsMeasurement.size());
        Assert.assertEquals(1.0, resourceSpansErrorsMeasurement.get(0).getValue(), 0);
        Assert.assertEquals(1, totalErrorsMeasurement.size());
        Assert.assertEquals(1.0, totalErrorsMeasurement.get(0).getValue(), 0);
    }

    @Test
    public void testEmptyCollection() {
        assertThat(oTelTraceRawPrepper.doExecute(Collections.EMPTY_LIST)).isEmpty();
    }

    @Test
    public void testEmptyTraceRequests() {
        assertThat(oTelTraceRawPrepper.doExecute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testEmptySpans() {
        assertThat(oTelTraceRawPrepper.doExecute(Arrays.asList(new Record<>(ExportTraceServiceRequest.newBuilder().build()),
                new Record<>(ExportTraceServiceRequest.newBuilder().build())))).isEmpty();
    }

    @Test
    public void testExportRequestFlushByParentSpan() throws IOException {
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TWO_FULL_TRACE_GROUP_JSON_FILE);
        oTelTraceRawPrepper.doExecute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));
        await().atMost(2 * TEST_ROOT_SPAN_FLUSH_DELAY, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<String>> processedRecords = (List<Record<String>>) oTelTraceRawPrepper.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(6);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(0);
        });
    }

    @Test
    public void testExportRequestFlushByParentSpanMultiThread() throws IOException, InterruptedException, ExecutionException {
        final ExportTraceServiceRequest exportTraceServiceRequest1 = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TWO_TRACE_GROUP_INTERLEAVED_JSON_FILE_1);
        final ExportTraceServiceRequest exportTraceServiceRequest2 = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TWO_TRACE_GROUP_INTERLEAVED_JSON_FILE_2);
        final List<Record<String>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest1)));
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest2)));
        for (Future<Collection<Record<String>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_ROOT_SPAN_FLUSH_DELAY, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<String>>>> futureList = submitExportTraceServiceRequests(Collections.emptyList());
            for (Future<Collection<Record<String>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(6);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(0);
        });
    }

    @Test
    public void testExportRequestFlushByGC() throws IOException {
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TWO_TRACE_GROUP_MISSING_ROOTS_JSON_FILE);
        oTelTraceRawPrepper.doExecute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<String>> processedRecords = (List<Record<String>>) oTelTraceRawPrepper.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(4);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(4);
        });
    }

    @Test
    public void testExportRequestFlushByMixedMultiThread() throws IOException, InterruptedException, ExecutionException {
        final ExportTraceServiceRequest exportTraceServiceRequest1 = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_ONE_FULL_TRACE_GROUP_JSON_FILE);
        final ExportTraceServiceRequest exportTraceServiceRequest2 = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_ONE_TRACE_GROUP_MISSING_ROOT_JSON_FILE);
        List<Record<String>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest1)));
        futures.addAll(submitExportTraceServiceRequests(Collections.singletonList(exportTraceServiceRequest2)));
        for (Future<Collection<Record<String>>> future : futures) {
            processedRecords.addAll(future.get());
        }
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            List<Future<Collection<Record<String>>>> futureList = submitExportTraceServiceRequests(Collections.emptyList());
            for (Future<Collection<Record<String>>> future : futureList) {
                processedRecords.addAll(future.get());
            }
            Assertions.assertThat(processedRecords.size()).isEqualTo(5);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(2);
        });
    }

    @Test
    public void testPrepareForShutdown() throws Exception {
        // Assert no records in memory
        assertTrue(oTelTraceRawPrepper.isReadyForShutdown());

        // Add records to memory/queue
        final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TWO_FULL_TRACE_GROUP_JSON_FILE);
        oTelTraceRawPrepper.doExecute(Collections.singletonList(new Record<>(exportTraceServiceRequest)));

        // Assert records exist in memory
        assertFalse(oTelTraceRawPrepper.isReadyForShutdown());

        // Force records to be flushed
        oTelTraceRawPrepper.prepareForShutdown();
        oTelTraceRawPrepper.doExecute(Collections.emptyList());

        // Assert records have been flushed
        assertTrue(oTelTraceRawPrepper.isReadyForShutdown());
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(requestJsonFileName))){
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        final String requestJson = jsonBuilder.toString();
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(requestJson, builder);
        return builder.build();
    }

    private List<Future<Collection<Record<String>>>> submitExportTraceServiceRequests(Collection<ExportTraceServiceRequest> exportTraceServiceRequests) {
        final List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        final List<Record<ExportTraceServiceRequest>> records = exportTraceServiceRequests.stream().map(Record::new).collect(Collectors.toList());
        futures.add(executorService.submit(() -> oTelTraceRawPrepper.doExecute(records)));
        return futures;
    }

    private int getMissingTraceGroupFieldsSpanCount(List<Record<String>> records) throws JsonProcessingException {
        int count = 0;
        for (Record<String> record: records) {
            final String spanJson = record.getData();
            Map<String, Object> spanMap = OBJECT_MAPPER.readValue(spanJson, new TypeReference<Map<String, Object>>() {});
            final String traceGroupName = (String) spanMap.get("traceGroup.name");
            final String traceGroupEndTime = (String) spanMap.get("traceGroup.endTime");
            final Number traceGroupDurationInNanos = (Number) spanMap.get("traceGroup.durationInNanos");
            final Number traceGroupStatusCode = (Number) spanMap.get("traceGroup.statusCode");
            if (Stream.of(traceGroupName, traceGroupEndTime, traceGroupDurationInNanos, traceGroupStatusCode).allMatch(Objects::isNull)) {
                count += 1;
            }
        }
        return count;
    }
}

