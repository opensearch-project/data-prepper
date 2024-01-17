/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.oteltracegroup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Measurement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.document.DocumentField;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.processor.oteltracegroup.model.TraceGroup;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OTelTraceGroupProcessorTests {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_PIPELINE_NAME = "testPipelineName";
    private static final String PLUGIN_NAME = "otel_trace_group";
    private static final String TEST_TRACE_ID_1 = "6d0ff634d126b6ec2c180391e67b4237";
    private static final TraceGroup TEST_TRACE_GROUP_1 = new TraceGroup.TraceGroupBuilder()
            .setTraceGroup("/test_trace_group_1")
            .setTraceGroupFields(DefaultTraceGroupFields.builder()
                    .withEndTime("2020-08-19T05:30:46.089556800Z")
                    .withStatusCode(1)
                    .withDurationInNanos(48545100L)
            .build())
            .build();
    private static final String TEST_TRACE_ID_2 = "ffa576d321173ac6cef3601c8f4bde75";
    private static final TraceGroup TEST_TRACE_GROUP_2 = new TraceGroup.TraceGroupBuilder()
            .setTraceGroup("/test_trace_group_2")
            .setTraceGroupFields(DefaultTraceGroupFields.builder()
                    .withEndTime("2020-08-20T05:30:46.089556800Z")
                    .withStatusCode(0)
                    .withDurationInNanos(48545300L)
                    .build())
            .build();
    private static final String TEST_RAW_SPAN_COMPLETE_JSON_FILE_1 = "raw-span-complete-1.json";
    private static final String TEST_RAW_SPAN_COMPLETE_JSON_FILE_2 = "raw-span-complete-2.json";
    private static final String TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1 = "raw-span-missing-trace-group-1.json";
    private static final String TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_2 = "raw-span-missing-trace-group-2.json";
    private static final int TEST_NUM_WORKERS = 2;

    private MockedStatic<ConnectionConfiguration> connectionConfigurationMockedStatic;

    private OTelTraceGroupProcessor otelTraceGroupProcessor;
    private ExecutorService executorService;

    @Mock
    private ConnectionConfiguration connectionConfigurationMock;

    @Mock(lenient = true)
    private RestHighLevelClient restHighLevelClient;

    @Mock(lenient = true)
    private SearchResponse testSearchResponse;

    @Mock(lenient = true)
    private SearchHits testSearchHits;

    @Mock(lenient = true)
    private SearchHit testSearchHit1;

    @Mock(lenient = true)
    private SearchHit testSearchHit2;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @BeforeEach
    void setUp() throws Exception{
        MetricsTestUtil.initMetrics();
        connectionConfigurationMockedStatic = Mockito.mockStatic(ConnectionConfiguration.class);
        connectionConfigurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(any(PluginSetting.class)))
                .thenReturn(connectionConfigurationMock);
        when(connectionConfigurationMock.createClient(awsCredentialsSupplier)).thenReturn(restHighLevelClient);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        doNothing().when(restHighLevelClient).close();
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {testSearchHit1});
        when(testSearchHit1.field("traceId")).thenReturn(new DocumentField("traceId", Collections.singletonList(TEST_TRACE_ID_1)));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_NAME_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_NAME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getTraceGroup())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_END_TIME_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_END_TIME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getTraceGroupFields().getEndTime())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getTraceGroupFields().getDurationInNanos())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getTraceGroupFields().getStatusCode())));
        when(testSearchHit2.field("traceId")).thenReturn(new DocumentField("traceId", Collections.singletonList(TEST_TRACE_ID_2)));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_NAME_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_NAME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getTraceGroup())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_END_TIME_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_END_TIME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getTraceGroupFields().getEndTime())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getTraceGroupFields().getDurationInNanos())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD))
                .thenReturn(new DocumentField(
                        TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getTraceGroupFields().getStatusCode())));
        final PluginSetting testPluginSetting = mock(PluginSetting.class);
        when(testPluginSetting.getName()).thenReturn(PLUGIN_NAME);
        when(testPluginSetting.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        otelTraceGroupProcessor = new OTelTraceGroupProcessor(testPluginSetting, awsCredentialsSupplier);
        executorService = Executors.newFixedThreadPool(TEST_NUM_WORKERS);
    }

    @AfterEach
    void tearDown() {
        otelTraceGroupProcessor.shutdown();
        connectionConfigurationMockedStatic.close();
        executorService.shutdown();
    }

    @Test
    void testShutDown() throws IOException {
        // Act
        otelTraceGroupProcessor.shutdown();

        // Assert
        verify(restHighLevelClient, times(1)).close();
    }

    @Test
    void testTraceGroupFillSuccess() throws IOException {
        // Arrange
        Record<Span> testRecord = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<Span>> testRecords = Collections.singletonList(testRecord);

        // Act
        List<Record<Span>> recordsOut = (List<Record<Span>>) otelTraceGroupProcessor.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<Span> recordOut = recordsOut.get(0);
        assertEquals(TEST_TRACE_GROUP_1, extractTraceGroupFromRecord(recordOut));
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_FIXED_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_MISSING_TRACE_GROUP, 0.0);
    }

    @Test
    void testTraceGroupFillFailDueToFailedRequest() throws IOException {
        // Arrange
        Record<Span> testRecord = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<Span>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class)))
                .thenThrow(new OpenSearchException("Failure due to search request"));

        // Act
        List<Record<Span>> recordsOut = (List<Record<Span>>) otelTraceGroupProcessor.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<Span> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_MISSING_TRACE_GROUP, 1.0);
    }

    @Test
    void testTraceGroupFillFailDueToNoHits() throws IOException {
        // Arrange
        Record<Span> testRecord = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<Span>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {});

        // Act
        List<Record<Span>> recordsOut = (List<Record<Span>>) otelTraceGroupProcessor.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<Span> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_MISSING_TRACE_GROUP, 1.0);
    }

    @Test
    void testTraceGroupFieldAlreadyPopulated() throws IOException {
        // Arrange
        Record<Span> testRecord = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_COMPLETE_JSON_FILE_1);
        List<Record<Span>> testRecords = Collections.singletonList(testRecord);

        // Act
        List<Record<Span>> recordsOut = (List<Record<Span>>) otelTraceGroupProcessor.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<Span> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_IN_MISSING_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupProcessor.RECORDS_OUT_MISSING_TRACE_GROUP, 0.0);
    }

    @Test
    void testTraceGroupProcessMultiWorker() throws IOException, ExecutionException, InterruptedException {
        /*
         * Note: we only test the threadsafety of the business logic in OtelTraceGroupProcessor. The OpenSearch REST client
         * itself is thread-safe {https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_changing_the_client_8217_s_initialization_code.html}.
         */
        // Arrange
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {testSearchHit1, testSearchHit2});
        Record<Span> testCompleteRecord1 = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_COMPLETE_JSON_FILE_1);
        Record<Span> testMissingRecord1 = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        Record<Span> testCompleteRecord2 = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_COMPLETE_JSON_FILE_2);
        Record<Span> testMissingRecord2 = buildSpanRecordFromJsonFile(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_2);
        final List<Record<Span>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();

        // Act
        futures.addAll(submitBatchRecords(Arrays.asList(testCompleteRecord1, testMissingRecord1)));
        futures.addAll(submitBatchRecords(Arrays.asList(testCompleteRecord2, testMissingRecord2)));
        for (Future<Collection<Record<Span>>> future : futures) {
            processedRecords.addAll(future.get());
        }

        // Assert
        assertEquals(4, processedRecords.size());
        for (Record<Span> record: processedRecords) {
            assertNotNull(extractTraceGroupFromRecord(record));
        }
    }

    @Test
    void testPrepareForShutdown() {
        otelTraceGroupProcessor.prepareForShutdown();

        assertTrue(otelTraceGroupProcessor.isReadyForShutdown());
    }

    private Record<Span> buildSpanRecordFromJsonFile(final String jsonFileName) throws IOException {
        JacksonSpan.Builder spanBuilder = JacksonSpan.builder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelTraceGroupProcessorTests.class.getClassLoader().getResourceAsStream(jsonFileName))){
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
            final String traceGroup = (String) spanMap.get(TraceGroup.TRACE_GROUP_NAME_FIELD);
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
                    .withTraceGroup(traceGroup);
            DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
            if (traceGroup != null) {
                final Integer traceGroupFieldsStatusCode = ((Number) spanMap.get(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD)).intValue();
                final String traceGroupFieldsEndTime = (String) spanMap.get(TraceGroup.TRACE_GROUP_END_TIME_FIELD);
                final Long traceGroupFieldsDurationInNanos = ((Number) spanMap.get(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD)).longValue();
                traceGroupFieldsBuilder = traceGroupFieldsBuilder
                        .withStatusCode(traceGroupFieldsStatusCode)
                        .withEndTime(traceGroupFieldsEndTime)
                        .withDurationInNanos(traceGroupFieldsDurationInNanos);
            }
            spanBuilder = spanBuilder
                    .withTraceGroup(traceGroup)
                    .withTraceGroupFields(traceGroupFieldsBuilder.build());
        }
        return new Record<>(spanBuilder.build());
    }

    private TraceGroup extractTraceGroupFromRecord(final Record<Span> record) {
        final Span span = record.getData();
        return new TraceGroup.TraceGroupBuilder()
                .setTraceGroup(span.getTraceGroup())
                .setTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withEndTime(span.getTraceGroupFields().getEndTime())
                        .withStatusCode(span.getTraceGroupFields().getStatusCode())
                        .withDurationInNanos(span.getTraceGroupFields().getDurationInNanos())
                        .build())
                .build();
    }

    private List<Future<Collection<Record<Span>>>> submitBatchRecords(List<Record<Span>> records) {
        final List<Future<Collection<Record<Span>>>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> otelTraceGroupProcessor.doExecute(records)));
        return futures;
    }

    private void checkMeasurementValue(final String name, final double expectedValue) {
        final List<Measurement> spansMissingTraceGroupMeasures = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(TEST_PIPELINE_NAME).add(PLUGIN_NAME).add(name).toString());
        assertEquals(1, spansMissingTraceGroupMeasures.size());
        final Measurement spansMissingTraceGroupMeasure = spansMissingTraceGroupMeasures.get(0);
        assertEquals(expectedValue, spansMissingTraceGroupMeasure.getValue(), 0);
    }
}
