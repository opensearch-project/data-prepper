package com.amazon.dataprepper.plugins.prepper.oteltracegroup;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.sink.elasticsearch.ConnectionConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class OtelTraceGroupPrepperTests {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_TRACE_GROUP = "/test_trace_group";
    private static final String TEST_RAW_SPAN_COMPLETE_JSON_FILE = "raw-span-complete.json";
    private static final String TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE = "raw-span-missing-trace-group.json";

    private MockedStatic<ConnectionConfiguration> connectionConfigurationMockedStatic;

    private OtelTraceGroupPrepper otelTraceGroupPrepper;

    @Mock
    private ConnectionConfiguration connectionConfigurationMock;

    @Mock
    private RestHighLevelClient restHighLevelClient;

    @Mock
    private SearchResponse testSearchResponse;

    @Mock
    private SearchHits testSearchHits;

    @Mock
    private SearchHit testSearchHit;

    @Before
    public void setUp() throws Exception{
        connectionConfigurationMockedStatic = Mockito.mockStatic(ConnectionConfiguration.class);
        connectionConfigurationMockedStatic.when(() -> ConnectionConfiguration.readConnectionConfiguration(any(PluginSetting.class)))
                .thenReturn(connectionConfigurationMock);
        when(connectionConfigurationMock.createClient()).thenReturn(restHighLevelClient);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        doNothing().when(restHighLevelClient).close();
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {testSearchHit});
        when(testSearchHit.field("traceGroup")).thenReturn(new DocumentField("traceGroup", Collections.singletonList(TEST_TRACE_GROUP)));
        final PluginSetting testPluginSetting = new PluginSetting("otel_trace_group_prepper", new HashMap<>()) {{
            setPipelineName("testPipelineName");
        }};
        otelTraceGroupPrepper = new OtelTraceGroupPrepper(testPluginSetting);
    }

    @After
    public void tearDown() {
        otelTraceGroupPrepper.shutdown();
        connectionConfigurationMockedStatic.close();
    }

    @Test
    public void testShutDown() throws IOException {
        // When
        otelTraceGroupPrepper.shutdown();

        // Then
        verify(restHighLevelClient, times(1)).close();
    }

    @Test
    public void testTraceGroupFillSuccess() throws IOException {
        // Given
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);

        // When
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Then
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(TEST_TRACE_GROUP, extractTraceGroupFromRecord(recordOut));
    }

    @Test
    public void testTraceGroupFillFailDueToRequest() throws IOException {
        // Given
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class)))
                .thenThrow(new ElasticsearchException("Failure due to search request"));

        // When
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Then
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
    }

    @Test
    public void testTraceGroupFillFailDueToNoHits() throws IOException {
        // Given
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_MISSING_TRACE_GROUP_JSON_FILE);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);
        when(restHighLevelClient.search(any(SearchRequest.class), any(RequestOptions.class))).thenReturn(testSearchResponse);
        when(testSearchResponse.getHits()).thenReturn(testSearchHits);
        when(testSearchHits.getHits()).thenReturn(new SearchHit[] {});

        // When
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Then
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
    }

    @Test
    public void testTraceGroupNoProcess() throws IOException {
        // Given
        Record<String> testRecord = buildRawSpanRecord(TEST_RAW_SPAN_COMPLETE_JSON_FILE);
        List<Record<String>> testRecords = Collections.singletonList(testRecord);

        // When
        List<Record<String>> recordsOut = (List<Record<String>>) otelTraceGroupPrepper.doExecute(testRecords);

        // Then
        assertEquals(1, recordsOut.size());
        Record<String> recordOut = recordsOut.get(0);
        assertEquals(testRecord, recordOut);
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

    private String extractTraceGroupFromRecord(final Record<String> record) throws JsonProcessingException {
        Map<String, Object> rawSpanMap = OBJECT_MAPPER.readValue(record.getData(), new TypeReference<Map<String, Object>>() {});
        return (String) rawSpanMap.get("traceGroup");
    }
}
