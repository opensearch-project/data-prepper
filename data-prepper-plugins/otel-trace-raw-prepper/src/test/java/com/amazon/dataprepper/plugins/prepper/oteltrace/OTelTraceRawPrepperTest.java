/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.trace.DefaultTraceGroupFields;
import com.amazon.dataprepper.model.trace.JacksonSpan;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.model.trace.TraceGroupFields;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.Status;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class OTelTraceRawPrepperTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long TEST_TRACE_FLUSH_INTERVAL = 3L;
    private static final int TEST_CONCURRENCY_SCALE = 2;

    private static final long TEST_ROOT_START_TIME_NANOS = 1597902043168010200L;
    private static final long TEST_ROOT_END_TIME_NANOS = 1597902043217170200L;
    private static final long TEST_CHILD_START_TIME_NANOS = 1597902043178010200L;
    private static final long TEST_CHILD_END_TIME_NANOS = 1597902043207170200L;

    private static final Span TEST_TRACE_GROUP_1_ROOT_SPAN = buildSpan("TRACE_ID_1", "TRACE_ID_1_ROOT_SPAN_ID",
            "", "TRACE_STATE_1", "TRACE_1_ROOT_SPAN", "SPAN_KIND_CLIENT",
            convertUnixNanosToISO8601(TEST_ROOT_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_ROOT_END_TIME_NANOS),
            TEST_ROOT_END_TIME_NANOS - TEST_ROOT_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_OK_VALUE);
    private static final Span TEST_TRACE_GROUP_1_CHILD_SPAN_1 = buildSpan("TRACE_ID_1", "TRACE_ID_1_CHILD_SPAN_ID_1",
            "TRACE_ID_1_ROOT_SPAN_ID", "TRACE_STATE_1", "TRACE_1_CHILD_SPAN_1", "SPAN_KIND_SERVER",
            convertUnixNanosToISO8601(TEST_CHILD_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_CHILD_END_TIME_NANOS),
            TEST_CHILD_END_TIME_NANOS - TEST_CHILD_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_OK_VALUE);
    private static final Span TEST_TRACE_GROUP_1_CHILD_SPAN_2 = buildSpan("TRACE_ID_1", "TRACE_ID_1_CHILD_SPAN_ID_2",
            "TRACE_ID_1_CHILD_SPAN_ID_1", "TRACE_STATE_1", "TRACE_1_CHILD_SPAN_2", "SPAN_KIND_SERVER",
            convertUnixNanosToISO8601(TEST_CHILD_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_CHILD_END_TIME_NANOS),
            TEST_CHILD_END_TIME_NANOS - TEST_CHILD_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_OK_VALUE);
    private static final Span TEST_TRACE_GROUP_2_ROOT_SPAN = buildSpan("TRACE_ID_2", "TRACE_ID_2_ROOT_SPAN_ID",
            "", "TRACE_STATE_2", "TRACE_2_ROOT_SPAN", "SPAN_KIND_CLIENT",
            convertUnixNanosToISO8601(TEST_ROOT_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_ROOT_END_TIME_NANOS),
            TEST_ROOT_END_TIME_NANOS - TEST_ROOT_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_ERROR_VALUE);
    private static final Span TEST_TRACE_GROUP_2_CHILD_SPAN_1 = buildSpan("TRACE_ID_2", "TRACE_ID_2_CHILD_SPAN_ID_1",
            "TRACE_ID_2_ROOT_SPAN_ID", "TRACE_STATE_2", "TRACE_2_CHILD_SPAN_1", "SPAN_KIND_SERVER",
            convertUnixNanosToISO8601(TEST_CHILD_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_CHILD_END_TIME_NANOS),
            TEST_CHILD_END_TIME_NANOS - TEST_CHILD_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_ERROR_VALUE);
    private static final Span TEST_TRACE_GROUP_2_CHILD_SPAN_2 = buildSpan("TRACE_ID_2", "TRACE_ID_2_CHILD_SPAN_ID_2",
            "TRACE_ID_2_ROOT_SPAN_ID", "TRACE_STATE_2", "TRACE_2_CHILD_SPAN_2", "SPAN_KIND_SERVER",
            convertUnixNanosToISO8601(TEST_CHILD_START_TIME_NANOS), convertUnixNanosToISO8601(TEST_CHILD_END_TIME_NANOS),
            TEST_CHILD_END_TIME_NANOS - TEST_CHILD_START_TIME_NANOS, Status.StatusCode.STATUS_CODE_ERROR_VALUE);

    private static final List<Record<Span>> TEST_ONE_FULL_TRACE_GROUP_RECORDS = Stream.of(
            TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());
    private static final List<Record<Span>> TEST_ONE_TRACE_GROUP_MISSING_ROOT_RECORDS = Stream.of(
                    TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());
    private static final List<Record<Span>> TEST_TWO_FULL_TRACE_GROUP_RECORDS = Stream.of(
                    TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2,
                    TEST_TRACE_GROUP_2_ROOT_SPAN, TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());
    private static final List<Record<Span>> TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_1_RECORDS = Stream.of(
                    TEST_TRACE_GROUP_1_ROOT_SPAN, TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());
    private static final List<Record<Span>> TEST_TWO_TRACE_GROUP_INTERLEAVED_PART_2_RECORDS = Stream.of(
                    TEST_TRACE_GROUP_2_ROOT_SPAN, TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());
    private static final List<Record<Span>> TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS = Stream.of(
                    TEST_TRACE_GROUP_1_CHILD_SPAN_1, TEST_TRACE_GROUP_1_CHILD_SPAN_2,
                    TEST_TRACE_GROUP_2_CHILD_SPAN_1, TEST_TRACE_GROUP_2_CHILD_SPAN_2)
            .map(Record::new).collect(Collectors.toList());

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
    public void testEmptyCollection() {
        assertThat(oTelTraceRawPrepper.doExecute(Collections.EMPTY_LIST)).isEmpty();
    }

    @Test
    public void testExportRequestFlushByParentSpan() {
        final List<Record<Span>> processedRecords = (List<Record<Span>>) oTelTraceRawPrepper.doExecute(TEST_TWO_FULL_TRACE_GROUP_RECORDS);

        Assertions.assertThat(processedRecords.size()).isEqualTo(6);
        Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(0);
    }

    @Test
    public void testExportRequestFlushByParentSpanMultiThread() throws InterruptedException, ExecutionException {
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
    public void testExportRequestFlushByGC() {
        oTelTraceRawPrepper.doExecute(TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS);
        await().atMost(2 * TEST_TRACE_FLUSH_INTERVAL, TimeUnit.SECONDS).untilAsserted(() -> {
            final List<Record<Span>> processedRecords = (List<Record<Span>>) oTelTraceRawPrepper.doExecute(Collections.emptyList());
            Assertions.assertThat(processedRecords.size()).isEqualTo(4);
            Assertions.assertThat(getMissingTraceGroupFieldsSpanCount(processedRecords)).isEqualTo(4);
        });
    }

    @Test
    public void testExportRequestFlushByMixedMultiThread() throws InterruptedException, ExecutionException {
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
    public void testPrepareForShutdown() {
        // Assert no records in memory
        assertTrue(oTelTraceRawPrepper.isReadyForShutdown());

        // Add records to memory/queue
        oTelTraceRawPrepper.doExecute(TEST_TWO_TRACE_GROUP_MISSING_ROOT_RECORDS);

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

    private static Span buildSpan(
            final String traceId,
            final String spanId,
            final String parentSpanId,
            final String traceState,
            final String name,
            final String kind,
            final String startTime,
            final String endTime,
            final Long durationInNanos,
            final Integer statusCode) {
        JacksonSpan.Builder spanBuilder = JacksonSpan.builder()
                .withTraceId(traceId)
                .withSpanId(spanId)
                .withParentSpanId(parentSpanId)
                .withTraceState(traceState)
                .withName(name)
                .withKind(kind)
                .withDurationInNanos(durationInNanos)
                .withStartTime(startTime)
                .withEndTime(endTime);
        if (parentSpanId.isEmpty()) {
            final DefaultTraceGroupFields traceGroupFields = DefaultTraceGroupFields.builder()
                    .withStatusCode(statusCode)
                    .withEndTime(endTime)
                    .withDurationInNanos(durationInNanos).build();
            spanBuilder = spanBuilder
                    .withTraceGroup(name)
                    .withTraceGroupFields(traceGroupFields);
        }
        return spanBuilder.build();
    }

    private static String convertUnixNanosToISO8601(final long unixNano) {
        return Instant.ofEpochSecond(0L, unixNano).toString();
    }

    private List<Future<Collection<Record<Span>>>> submitRecords(Collection<Record<Span>> records) {
        final List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> oTelTraceRawPrepper.doExecute(records)));
        return futures;
    }

    private int getMissingTraceGroupFieldsSpanCount(List<Record<Span>> records) {
        int count = 0;
        for (Record<Span> record: records) {
            final Span span = record.getData();
            final String traceGroup = span.getTraceGroup();
            final TraceGroupFields traceGroupFields = span.getTraceGroupFields();
            if (Stream.of(traceGroup, traceGroupFields).allMatch(Objects::isNull)) {
                count += 1;
            }
        }
        return count;
    }
}

