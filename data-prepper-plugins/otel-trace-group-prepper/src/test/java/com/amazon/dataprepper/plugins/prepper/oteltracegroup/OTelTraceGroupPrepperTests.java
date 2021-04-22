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

package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.prepper.oteltracegroup.model.TraceGroup;
import com.amazon.dataprepper.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OTelTraceGroupPrepperTests {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_PIPELINE_NAME = "testPipelineName";
    private static final String PLUGIN_NAME = "otel_trace_group_prepper";
    private static final String TEST_TRACE_ID_1 = "6d0ff634d126b6ec2c180391e67b4237";
    private static final TraceGroup TEST_TRACE_GROUP_1 = new TraceGroup("/test_trace_group_1",
            "2020-08-19T05:30:46.089556800Z", 48545100L, 1);
    private static final String TEST_TRACE_ID_2 = "ffa576d321173ac6cef3601c8f4bde75";
    private static final TraceGroup TEST_TRACE_GROUP_2 = new TraceGroup("/test_trace_group_2",
            "2020-08-20T05:30:46.089556800Z", 48545300L, 0);
    private static final String TEST_RAW_SPAN_COMPLETE_JSON_FILE_1 = "raw-span-complete-1.json";
    private static final String TEST_RAW_SPAN_COMPLETE_JSON_FILE_2 = "raw-span-complete-2.json";
    private static final String TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1 = "raw-span-missing-trace-group-1.json";
    private static final String TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_2 = "raw-span-missing-trace-group-2.json";
    private static final int TEST_NUM_WORKERS = 2;

    private MockedStatic<ConnectionConfiguration> connectionConfigurationMockedStatic;

    private OTelTraceGroupPrepper otelTraceGroupPrepper;
    private ExecutorService executorService;

    @Mock
    private ConnectionConfiguration connectionConfigurationMock;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private SearchResponse testSearchResponse;

    @Mock
    private SearchHits testSearchHits;

    @Mock
    private SearchHit testSearchHit1;

    @Mock
    private SearchHit testSearchHit2;

    @Before
    public void setUp() throws Exception{
        MetricsTestUtil.initMetrics();
        connectionConfigurationMockedStatic = Mockito.mockStatic(ConnectionConfiguration.class);
        connectionConfigurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(any(PluginSetting.class)))
                .thenReturn(connectionConfigurationMock);
        when(connectionConfigurationMock.createClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        doNothing().when(restHighLevelClient).close();
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {testSearchHit1});
        when(testSearchHit1.field("traceId")).thenReturn(new DocumentField("traceId", Collections.singletonList(TEST_TRACE_ID_1)));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_NAME_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_NAME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getName())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_END_TIME_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_END_TIME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getEndTime())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getDurationInNanos())));
        when(testSearchHit1.field(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD, Collections.singletonList(TEST_TRACE_GROUP_1.getStatusCode())));
        when(testSearchHit2.field("traceId")).thenReturn(new DocumentField("traceId", Collections.singletonList(TEST_TRACE_ID_2)));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_NAME_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_NAME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getName())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_END_TIME_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_END_TIME_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getEndTime())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getDurationInNanos())));
        when(testSearchHit2.field(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD))
                .thenReturn(new DocumentField(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD, Collections.singletonList(TEST_TRACE_GROUP_2.getStatusCode())));
        final PluginSetting testPluginSetting = new PluginSetting("otel_trace_group_prepper", new HashMap<>()) {{
            setPipelineName(TEST_PIPELINE_NAME);
        }};
        otelTraceGroupPrepper = new OTelTraceGroupPrepper(testPluginSetting);
        executorService = Executors.newFixedThreadPool(TEST_NUM_WORKERS);
    }

    @After
    public void tearDown() {
        otelTraceGroupPrepper.shutdown();
        connectionConfigurationMockedStatic.close();
        executorService.shutdown();
    }

    @Test
    public void testShutDown() throws IOException {
        // Act
        otelTraceGroupPrepper.shutdown();

        // Assert
        verify(restHighLevelClient, times(1)).close();
    }

    @Test
    public void testTraceGroupFillSuccess() throws IOException {
        // Arrange
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);

        // Act
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(TEST_TRACE_GROUP_1, extractTraceGroupFromRecord(recordOut));
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_FIXED_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_MISSING_TRACE_GROUP, 0.0);
    }

    @Test
    public void testTraceGroupFillFailDueToFailedRequest() throws IOException {
        // Arrange
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class)))
                .thenThrow(new OpenSearchException("Failure due to search request"));

        // Act
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_MISSING_TRACE_GROUP, 1.0);
    }

    @Test
    public void testTraceGroupFillFailDueToNoHits() throws IOException {
        // Arrange
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {});

        // Act
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_IN_MISSING_TRACE_GROUP, 1.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_MISSING_TRACE_GROUP, 1.0);
    }

    @Test
    public void testTraceGroupFieldAlreadyPopulated() throws IOException {
        // Arrange
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_COMPLETE_JSON_FILE_1);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);

        // Act
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Assert
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_IN_MISSING_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_FIXED_TRACE_GROUP, 0.0);
        checkMeasurementValue(OTelTraceGroupPrepper.RECORDS_OUT_MISSING_TRACE_GROUP, 0.0);
    }

    @Test
    public void testTraceGroupProcessMultiWorker() throws IOException, ExecutionException, InterruptedException {
        /*
         * Note: we only test the threadsafety of the business logic in OtelTraceGroupPrepper. The elasticsearch REST client
         * itself is thread-safe {https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/_changing_the_client_8217_s_initialization_code.html}.
         */
        // Arrange
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {testSearchHit1, testSearchHit2});
        Record<String> testCompleteRecord1 = buildRawSpanRecord(TEST_RAW_SPAN_COMPLETE_JSON_FILE_1);
        Record<String> testMissingRecord1 = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_1);
        Record<String> testCompleteRecord2 = buildRawSpanRecord(TEST_RAW_SPAN_COMPLETE_JSON_FILE_2);
        Record<String> testMissingRecord2 = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE_2);
        final List<Record<String>> processedRecords = new ArrayList<>();
        List<Future<Collection<Record<String>>>> futures = new ArrayList<>();

        // Act
        futures.addAll(submitBatchRecords(Arrays.asList(testCompleteRecord1, testMissingRecord1)));
        futures.addAll(submitBatchRecords(Arrays.asList(testCompleteRecord2, testMissingRecord2)));
        for (Future<Collection<Record<String>>> future : futures) {
            processedRecords.addAll(future.get());
        }

        // Assert
        assertEquals(4, processedRecords.size());
        for (Record<String> record: processedRecords) {
            assertNotNull(extractTraceGroupFromRecord(record));
        }
    }

    @Test
    public void testPrepareForShutdown() {
        otelTraceGroupPrepper.prepareForShutdown();

        assertTrue(otelTraceGroupPrepper.isReadyForShutdown());
    }

    private Record<String> buildRawSpanRecord(String rawSpanJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(rawSpanJsonFileName))){
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return new Record<>(jsonBuilder.toString());
    }

    private TraceGroup extractTraceGroupFromRecord(final Record<String> record) throws JsonProcessingException {
        Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), new TypeReference<Map<String, Object>>() {});
        final String traceGroupName = (String) rawSpanMap.get(TraceGroup.TRACE_GROUP_NAME_FIELD);
        final String traceGroupEndTime = (String) rawSpanMap.get(TraceGroup.TRACE_GROUP_END_TIME_FIELD);
        final Long traceGroupDurationInNanos = ((Number) rawSpanMap.get(TraceGroup.TRACE_GROUP_DURATION_IN_NANOS_FIELD)).longValue();
        final Integer traceGroupStatusCode = ((Number) rawSpanMap.get(TraceGroup.TRACE_GROUP_STATUS_CODE_FIELD)).intValue();
        return new TraceGroup(traceGroupName, traceGroupEndTime, traceGroupDurationInNanos, traceGroupStatusCode);
    }

    private List<Future<Collection<Record<String>>>> submitBatchRecords(List<Record<String>> records) {
        final List<Future<Collection<Record<String>>>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> otelTraceGroupPrepper.doExecute(records)));
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
