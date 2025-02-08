/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.micrometer.core.instrument.Measurement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.AbstractIndexManager;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.http.HttpStatus.SC_OK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createContentParser;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createOpenSearchClient;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.getHosts;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.isOSBundle;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.waitForClusterStateUpdatesToFinish;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.wipeAllTemplates;

public class OpenSearchSinkIT {
    private static final int LUCENE_CHAR_LENGTH_LIMIT = 32_766;
    private static final String AUTHENTICATION = "authentication";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String TEST_STRING_WITH_SPECIAL_CHARS = "Hello! Data-Prepper? #Example123";
    private static final String TEST_STRING_WITH_NON_LATIN_CHARS = "Привет,Γειά σας,こんにちは,你好";
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "integTestPipeline";
    private static final String TEST_CUSTOM_INDEX_POLICY_FILE = "test-custom-index-policy-file.json";
    private static final String TEST_TEMPLATE_V1_FILE = "test-index-template.json";
    private static final String TEST_TEMPLATE_BULK_FILE = "test-bulk-template.json";
    private static final String TEST_TEMPLATE_V2_FILE = "test-index-template-v2.json";
    private static final String TEST_INDEX_TEMPLATE_V1_FILE = "test-composable-index-template.json";
    private static final String TEST_INDEX_TEMPLATE_V2_FILE = "test-composable-index-template-v2.json";
    private static final String DEFAULT_RAW_SPAN_FILE_1 = "raw-span-1.json";
    private static final String DEFAULT_RAW_SPAN_FILE_2 = "raw-span-2.json";
    private static final String DEFAULT_SERVICE_MAP_FILE = "service-map-1.json";
    private static final String INCLUDE_TYPE_NAME_FALSE_URI = "?include_type_name=false";
    private static final String TRACE_INGESTION_TEST_DISABLED_REASON = "Trace ingestion is not supported for ES 6";
    private static final String LOG_INGESTION_TEST_DISABLED_REASON = "Log ingestion is not supported for ES 6";
    private static final String METRIC_INGESTION_TEST_DISABLED_REASON = "Metric ingestion is not supported for ES 6";

    private RestClient client;
    private SinkContext sinkContext;
    private String testTagsTargetKey;

    ObjectMapper objectMapper;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private PluginConfigObservable pluginConfigObservable;

    public OpenSearchSink createObjectUnderTest(OpenSearchSinkConfig openSearchSinkConfig, boolean doInitialize) {
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        OpenSearchSink sink = new OpenSearchSink(
                pluginSetting, null, expressionEvaluator, awsCredentialsSupplier, pipelineDescription, pluginConfigObservable, openSearchSinkConfig);
        if (doInitialize) {
            sink.doInitialize();
        }
        return sink;
    }

    public OpenSearchSink createObjectUnderTestWithSinkContext(OpenSearchSinkConfig openSearchSinkConfig, boolean doInitialize) {
        sinkContext = mock(SinkContext.class);
        testTagsTargetKey = RandomStringUtils.randomAlphabetic(5);
        when(sinkContext.getTagsTargetKey()).thenReturn(testTagsTargetKey);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        OpenSearchSink sink = new OpenSearchSink(
                pluginSetting, sinkContext, expressionEvaluator, awsCredentialsSupplier, pipelineDescription, pluginConfigObservable, openSearchSinkConfig);
        if (doInitialize) {
            sink.doInitialize();
        }
        return sink;
    }

    @BeforeEach
    public void setup() {
        pluginConfigObservable = mock(PluginConfigObservable.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginSetting = mock(PluginSetting.class);
        when(expressionEvaluator.isValidExpressionStatement(any(String.class))).thenReturn(false);

    }

    @BeforeEach
    public void metricsInit() throws IOException {
        MetricsTestUtil.initMetrics();

        client = createOpenSearchClient();
    }

    @AfterEach
    public void cleanOpenSearch() throws Exception {
        wipeAllOpenSearchIndices();
        wipeAllTemplates();
        waitForClusterStateUpdatesToFinish();
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkRawSpanDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        assertThat(indexAlias, equalTo("otel-v1-apm-span"));
        Request request = new Request(HttpMethod.HEAD, indexAlias);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        final String index = String.format("%s-000001", indexAlias);
        final Map<String, Object> mappings = getIndexMappings(index);
        assertThat(mappings, notNullValue());
        assertThat((boolean) mappings.get("date_detection"), equalTo(false));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(getIndexPolicyId(index), equalTo(IndexConstants.RAW_ISM_POLICY));
                    }
            );
        }

        // roll over initial index
        request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
        request.setJsonEntity("{ \"conditions\" : { } }\n");
        response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Instantiate sink again
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        // Make sure no new write index *-000001 is created under alias
        final String rolloverIndexName = String.format("%s-000002", indexAlias);
        request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
        response = client.performRequest(request);
        assertThat(checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName), equalTo(true));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            assertThat(getIndexPolicyId(rolloverIndexName), equalTo(IndexConstants.RAW_ISM_POLICY));
        }
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = LOG_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkLogsDefaultLogSink() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.LOG_ANALYTICS.getValue(), null, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.LOG_ANALYTICS);
        assertThat(indexAlias, equalTo("logs-otel-v1"));
        Request request = new Request(HttpMethod.HEAD, indexAlias);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        final String index = String.format("%s-000001", indexAlias);
        final Map<String, Object> mappings = getIndexMappings(index);
        assertThat(mappings, notNullValue());
        assertThat((boolean) mappings.get("date_detection"), equalTo(false));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(getIndexPolicyId(index), equalTo(IndexConstants.LOGS_ISM_POLICY));
                    }
            );
        }

        // roll over initial index
        request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
        request.setJsonEntity("{ \"conditions\" : { } }\n");
        response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Instantiate sink again
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        // Make sure no new write index *-000001 is created under alias
        final String rolloverIndexName = String.format("%s-000002", indexAlias);
        request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
        response = client.performRequest(request);
        assertThat(checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName), equalTo(true));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            assertThat(getIndexPolicyId(rolloverIndexName), equalTo(IndexConstants.LOGS_ISM_POLICY));
        }
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = METRIC_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkMetricsDefaultMetricSink() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.METRIC_ANALYTICS.getValue(), null, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.METRIC_ANALYTICS);
        assertThat(indexAlias, equalTo("metrics-otel-v1"));
        Request request = new Request(HttpMethod.HEAD, indexAlias);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        final String index = String.format("%s-000001", indexAlias);
        final Map<String, Object> mappings = getIndexMappings(index);
        assertThat(mappings, notNullValue());
        assertThat((boolean) mappings.get("date_detection"), equalTo(false));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(getIndexPolicyId(index), equalTo(IndexConstants.METRICS_ISM_POLICY));
                    }
            );
        }

        // roll over initial index
        request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
        request.setJsonEntity("{ \"conditions\" : { } }\n");
        response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Instantiate sink again
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        // Make sure no new write index *-000001 is created under alias
        final String rolloverIndexName = String.format("%s-000002", indexAlias);
        request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
        response = client.performRequest(request);
        assertThat(checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName), equalTo(true));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            assertThat(getIndexPolicyId(rolloverIndexName), equalTo(IndexConstants.METRICS_ISM_POLICY));
        }
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkRawSpanReservedAliasAlreadyUsedAsIndex() throws IOException {

        final String reservedIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        final Request request = new Request(HttpMethod.PUT, reservedIndexAlias);
        client.performRequest(request);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, false);
        Assert.assertThrows(String.format(AbstractIndexManager.INDEX_ALIAS_USED_AS_INDEX_ERROR, reservedIndexAlias),
                RuntimeException.class, () -> sink.doInitialize());
    }

    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testOutputRawSpanDefault(final boolean estimateBulkSizeUsingCompression,
                                         final boolean isRequestCompressionEnabled) throws IOException, InterruptedException {
        final String testDoc1 = readDocFromFile(DEFAULT_RAW_SPAN_FILE_1);
        final String testDoc2 = readDocFromFile(DEFAULT_RAW_SPAN_FILE_2);
        final ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked") final Map<String, Object> expData1 = mapper.readValue(testDoc1, Map.class);
        @SuppressWarnings("unchecked") final Map<String, Object> expData2 = mapper.readValue(testDoc2, Map.class);

        final List<Record<Event>> testRecords = Arrays.asList(jsonStringToRecord(testDoc1), jsonStringToRecord(testDoc2));
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null,
                estimateBulkSizeUsingCompression, isRequestCompressionEnabled);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
        assertThat(retSources.size(), equalTo(2));
        assertThat(retSources, hasItems(expData1, expData2));
        assertThat(getDocumentCount(expIndexAlias, "_id", (String) expData1.get("spanId")), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // Verify metrics
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
        // TOTAL_TIME
        Assert.assertTrue(bulkRequestLatencies.get(1).getValue() > 0.0);
        // MAX
        Assert.assertTrue(bulkRequestLatencies.get(2).getValue() > 0.0);
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertThat(documentsSuccessMeasurements.size(), equalTo(1));
        assertThat(documentsSuccessMeasurements.get(0).getValue(), closeTo(2.0, 0));
        final List<Measurement> documentsSuccessFirstAttemptMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS_FIRST_ATTEMPT).toString());
        assertThat(documentsSuccessFirstAttemptMeasurements.size(), equalTo(1));
        assertThat(documentsSuccessFirstAttemptMeasurements.get(0).getValue(), closeTo(2.0, 0));
        final List<Measurement> documentErrorsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENT_ERRORS).toString());
        assertThat(documentErrorsMeasurements.size(), equalTo(1));
        assertThat(documentErrorsMeasurements.get(0).getValue(), closeTo(0.0, 0));

        /**
         * Metrics: Bulk Request Size in Bytes
         */
        final List<Measurement> bulkRequestSizeBytesMetrics = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_SIZE_BYTES).toString());
        assertThat(bulkRequestSizeBytesMetrics.size(), equalTo(3));
        assertThat(bulkRequestSizeBytesMetrics.get(0).getValue(), closeTo(1.0, 0));
        final double expectedBulkRequestSizeBytes = isRequestCompressionEnabled && estimateBulkSizeUsingCompression ? 792.0 : 2058.0;
        assertThat(bulkRequestSizeBytesMetrics.get(1).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));
        assertThat(bulkRequestSizeBytesMetrics.get(2).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));
    }

    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testOutputRawSpanWithDLQ(final boolean estimateBulkSizeUsingCompression,
                                         final boolean isRequestCompressionEnabled) throws IOException, InterruptedException {
        // TODO: write test case
        final String testDoc1 = readDocFromFile("raw-span-error.json");
        final String testDoc2 = readDocFromFile(DEFAULT_RAW_SPAN_FILE_1);
        final ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked") final Map<String, Object> expData = mapper.readValue(testDoc2, Map.class);

        final List<Record<Event>> testRecords = Arrays.asList(jsonStringToRecord(testDoc1), jsonStringToRecord(testDoc2));
        Map<String, Object> metadata = initializeConfigurationMetadata(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null,
                estimateBulkSizeUsingCompression, isRequestCompressionEnabled);
        // generate temporary directory for dlq file
        final File tempDirectory = Files.createTempDirectory("").toFile();
        // add dlq file path into setting
        final String expDLQFile = tempDirectory.getAbsolutePath() + "/test-dlq.txt";
        metadata.put(RetryConfiguration.DLQ_FILE, expDLQFile);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);

        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        sink.shutdown();

        final StringBuilder dlqContent = new StringBuilder();
        Files.lines(Paths.get(expDLQFile)).forEach(dlqContent::append);
        final String nonPrettyJsonString = mapper.writeValueAsString(mapper.readValue(testDoc1, JsonNode.class));
        assertThat(dlqContent.toString(), containsString(nonPrettyJsonString));
        final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.get(0), equalTo(expData));

        // clean up temporary directory
        FileUtils.deleteQuietly(tempDirectory);

        // verify metrics
        final List<Measurement> documentsSuccessMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertThat(documentsSuccessMeasurements.size(), equalTo(1));
        assertThat(documentsSuccessMeasurements.get(0).getValue(), closeTo(1.0, 0));
        final List<Measurement> documentErrorsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENT_ERRORS).toString());
        assertThat(documentErrorsMeasurements.size(), equalTo(1));
        assertThat(documentErrorsMeasurements.get(0).getValue(), closeTo(1.0, 0));

        /**
         * Metrics: Bulk Request Size in Bytes
         */
        final List<Measurement> bulkRequestSizeBytesMetrics = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_SIZE_BYTES).toString());
        assertThat(bulkRequestSizeBytesMetrics.size(), equalTo(3));
        assertThat(bulkRequestSizeBytesMetrics.get(0).getValue(), closeTo(1.0, 0));
        final double expectedBulkRequestSizeBytes = isRequestCompressionEnabled && estimateBulkSizeUsingCompression ? 1078.0 : 2072.0;
        assertThat(bulkRequestSizeBytesMetrics.get(1).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));
        assertThat(bulkRequestSizeBytesMetrics.get(2).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));

    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkServiceMapDefault() throws IOException {
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP);
        final Request request = new Request(HttpMethod.HEAD, indexAlias);
        final Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        final Map<String, Object> mappings = getIndexMappings(indexAlias);
        assertThat(mappings, notNullValue());
        assertThat((boolean) mappings.get("date_detection"), equalTo(false));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            assertThat(getIndexPolicyId(indexAlias), nullValue());
        }
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    public void testOutputServiceMapDefault(final boolean estimateBulkSizeUsingCompression,
                                            final boolean isRequestCompressionEnabled) throws IOException, InterruptedException {
        final String testDoc = readDocFromFile(DEFAULT_SERVICE_MAP_FILE);
        final ObjectMapper mapper = new ObjectMapper();
        @SuppressWarnings("unchecked") final Map<String, Object> expData = mapper.readValue(testDoc, Map.class);

        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(testDoc));
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_SERVICE_MAP.getValue(), null, null,
                estimateBulkSizeUsingCompression, isRequestCompressionEnabled);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_SERVICE_MAP);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.get(0), equalTo(expData));
        assertThat(getDocumentCount(expIndexAlias, "_id", (String) expData.get("hashId")), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        assertThat(bulkRequestLatencies.get(0).getValue(), closeTo(1.0, 0));

        /**
         * Metrics: Bulk Request Size in Bytes
         */
        final List<Measurement> bulkRequestSizeBytesMetrics = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_SIZE_BYTES).toString());
        assertThat(bulkRequestSizeBytesMetrics.size(), equalTo(3));
        assertThat(bulkRequestSizeBytesMetrics.get(0).getValue(), closeTo(1.0, 0));
        final double expectedBulkRequestSizeBytes = isRequestCompressionEnabled && estimateBulkSizeUsingCompression ? 376.0 : 265.0;
        assertThat(bulkRequestSizeBytesMetrics.get(1).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));
        assertThat(bulkRequestSizeBytesMetrics.get(2).getValue(), closeTo(expectedBulkRequestSizeBytes, 0));

        // Check restart for index already exists
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.shutdown();
    }

    @Test
    public void testInstantiateSinkCustomIndex_NoRollOver() throws IOException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, testTemplateFile);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        final Request request = new Request(HttpMethod.HEAD, testIndexAlias + extraURI);
        final Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        sink.shutdown();

        // Check restart for index already exists
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.shutdown();
    }

    @ParameterizedTest
    @ArgumentsSource(CreateSingleWithTemplatesArgumentsProvider.class)
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testInstantiateSinkCustomIndex_WithIsmPolicy(
            final String templateType,
            final String templateFile) throws IOException {
        final String indexAlias = "sink-custom-index-ism-test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(templateFile)).getFile();
        final Map<String, Object> metadata = initializeConfigurationMetadata(null, indexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.ISM_POLICY_FILE, TEST_CUSTOM_INDEX_POLICY_FILE);
        metadata.put(IndexConfiguration.TEMPLATE_TYPE, templateType);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);

        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        Request request = new Request(HttpMethod.HEAD, indexAlias + extraURI);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
        final String index = String.format("%s-000001", indexAlias);
        final Map<String, Object> mappings = getIndexMappings(index);
        assertThat(mappings, notNullValue());
        assertThat((boolean) mappings.get("date_detection"), equalTo(false));

        sink.shutdown();

        JsonNode settings = getIndexSettings(index);

        assertThat(settings, notNullValue());
        JsonNode settingsIndexNode = settings.get("index");
        assertThat(settingsIndexNode, notNullValue());
        assertThat(settingsIndexNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertThat(settingsIndexNode.get("opendistro"), notNullValue());
        assertThat(settingsIndexNode.get("opendistro").getNodeType(), equalTo(JsonNodeType.OBJECT));
        JsonNode settingsIsmNode = settingsIndexNode.get("opendistro").get("index_state_management");
        assertThat(settingsIsmNode, notNullValue());
        assertThat(settingsIsmNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertThat(settingsIsmNode.get("rollover_alias"), notNullValue());
        assertThat(settingsIsmNode.get("rollover_alias").getNodeType(), equalTo(JsonNodeType.STRING));
        assertThat(settingsIsmNode.get("rollover_alias").textValue(), equalTo(indexAlias));

        final String expectedIndexPolicyName = indexAlias + "-policy";
        if (isOSBundle()) {
            // Check managed index
            await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
                        assertThat(getIndexPolicyId(index), equalTo(expectedIndexPolicyName));
                    }
            );
        }

        // roll over initial index
        request = new Request(HttpMethod.POST, String.format("%s/_rollover", indexAlias));
        request.setJsonEntity("{ \"conditions\" : { } }\n");
        response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Instantiate sink again
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        // Make sure no new write index *-000001 is created under alias
        final String rolloverIndexName = String.format("%s-000002", indexAlias);
        request = new Request(HttpMethod.GET, rolloverIndexName + "/_alias");
        response = client.performRequest(request);
        assertThat(checkIsWriteIndex(EntityUtils.toString(response.getEntity()), indexAlias, rolloverIndexName), equalTo(true));
        sink.shutdown();

        if (isOSBundle()) {
            // Check managed index
            assertThat(getIndexPolicyId(rolloverIndexName), equalTo(expectedIndexPolicyName));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CreateWithTemplatesArgumentsProvider.class)
    public void testInstantiateSinkDoesNotOverwriteNewerIndexTemplates(
            final String templateType,
            final String templatePath,
            final String v1File,
            final String v2File,
            final BiFunction<Map<String, Object>, String, Integer> extractVersionFunction) throws IOException {
        final String testIndexAlias = "test-alias";
        final String expectedIndexTemplateName = testIndexAlias + "-index-template";
        final String testTemplateFileV1 = getClass().getClassLoader().getResource(v1File).getFile();
        final String testTemplateFileV2 = getClass().getClassLoader().getResource(v2File).getFile();

        // Create sink with template version 1
        OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, templateType, testTemplateFileV1);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);

        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        Request getTemplateRequest = new Request(HttpMethod.GET,
                "/" + templatePath + "/" + expectedIndexTemplateName + extraURI);
        Response getTemplateResponse = client.performRequest(getTemplateRequest);
        assertThat(getTemplateResponse.getStatusLine().getStatusCode(), equalTo(SC_OK));

        String responseBody = EntityUtils.toString(getTemplateResponse.getEntity());
        @SuppressWarnings("unchecked") final Integer firstResponseVersion =
                extractVersionFunction.apply(createContentParser(XContentType.JSON.xContent(),
                        responseBody).map(), expectedIndexTemplateName);

        assertThat(firstResponseVersion, equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // Create sink with template version 2
        openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, templateType, testTemplateFileV2);
        sink = createObjectUnderTest(openSearchSinkConfig, true);

        getTemplateRequest = new Request(HttpMethod.GET,
                "/" + templatePath + "/" + expectedIndexTemplateName + extraURI);
        getTemplateResponse = client.performRequest(getTemplateRequest);
        assertThat(getTemplateResponse.getStatusLine().getStatusCode(), equalTo(SC_OK));

        responseBody = EntityUtils.toString(getTemplateResponse.getEntity());
        @SuppressWarnings("unchecked") final Integer secondResponseVersion =
                extractVersionFunction.apply(createContentParser(XContentType.JSON.xContent(),
                        responseBody).map(), expectedIndexTemplateName);

        assertThat(secondResponseVersion, equalTo(Integer.valueOf(2)));
        sink.shutdown();

        // Create sink with template version 1 again
        openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, templateType, testTemplateFileV1);
        sink = createObjectUnderTest(openSearchSinkConfig, true);

        getTemplateRequest = new Request(HttpMethod.GET,
                "/" + templatePath + "/" + expectedIndexTemplateName + extraURI);
        getTemplateResponse = client.performRequest(getTemplateRequest);
        assertThat(getTemplateResponse.getStatusLine().getStatusCode(), equalTo(SC_OK));

        responseBody = EntityUtils.toString(getTemplateResponse.getEntity());
        @SuppressWarnings("unchecked") final Integer thirdResponseVersion =
                extractVersionFunction.apply(createContentParser(XContentType.JSON.xContent(),
                        responseBody).map(), expectedIndexTemplateName);

        // Assert version 2 was not overwritten by version 1
        assertThat(thirdResponseVersion, equalTo(Integer.valueOf(2)));
        sink.shutdown();

    }

    @ParameterizedTest
    @ArgumentsSource(CreateWithIndexTemplateArgumentsProvider.class)
    public void testIndexNameWithDateNotAsSuffixCreatesIndexTemplate(
            final String templateType,
            final String templatePath,
            final String templateFile,
            final BiFunction<Map<String, Object>, String, Integer> extractVersionFunction,
            final BiFunction<Map<String, Object>, String, Map<String, Object>> extractMappingsFunction) throws IOException {
        final String testIndexAlias = "prefix-%{yyyy-MM}-suffix";
        final String expectedDate = new SimpleDateFormat("yyyy-MM").format(new Date());
        final String expectedIndexName = "prefix-" + expectedDate + "-suffix";
        final String expectedIndexTemplateName = "prefix-suffix-index-template";
        final String testTemplateFileV1 = getClass().getClassLoader().getResource(templateFile).getFile();

        OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, templateType, testTemplateFileV1);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);

        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        Request getTemplateRequest = new Request(HttpMethod.GET,
                "/" + templatePath + "/" + expectedIndexTemplateName + extraURI);
        Response getTemplateResponse = client.performRequest(getTemplateRequest);
        assertThat(getTemplateResponse.getStatusLine().getStatusCode(), equalTo(SC_OK));

        String responseBody = EntityUtils.toString(getTemplateResponse.getEntity());
        @SuppressWarnings("unchecked") final Integer responseVersion =
                extractVersionFunction.apply(createContentParser(XContentType.JSON.xContent(),
                        responseBody).map(), expectedIndexTemplateName);
        @SuppressWarnings("unchecked") final Map<String, Object> templateMappings =
                extractMappingsFunction.apply(createContentParser(XContentType.JSON.xContent(),
                        responseBody).map(), expectedIndexTemplateName);
        assertThat(responseVersion, equalTo(Integer.valueOf(1)));
        assertThat(templateMappings.isEmpty(), equalTo(false));

        Request getIndexRequest = new Request(HttpMethod.GET,
                "/" + expectedIndexName + extraURI);
        Response getIndexResponse = client.performRequest(getIndexRequest);
        assertThat(getIndexResponse.getStatusLine().getStatusCode(), equalTo(SC_OK));

        String getIndexResponseBody = EntityUtils.toString(getIndexResponse.getEntity());
        @SuppressWarnings("unchecked") final Map<String, Object> indexBlob = (Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                        getIndexResponseBody).map().get(expectedIndexName);
        @SuppressWarnings("unchecked") final Map<String, Object> mappingsBlob = (Map<String, Object>) indexBlob.get("mappings");
        assertThat(mappingsBlob.isEmpty(), equalTo(false));

        // assert the mappings from index template are applied to the index
        assertThat(mappingsBlob, equalTo(templateMappings));

        sink.shutdown();
    }

    static class CreateWithTemplatesArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            final List<Arguments> arguments = new ArrayList<>();
            arguments.add(
                    arguments("v1", "_template",
                            TEST_TEMPLATE_V1_FILE, TEST_TEMPLATE_V2_FILE,
                            (BiFunction<Map<String, Object>, String, Integer>) (map, templateName) ->
                                    (Integer) ((Map<String, Object>) map.get(templateName)).get("version")
                    )
            );

            if (OpenSearchIntegrationHelper.getVersion().compareTo(DeclaredOpenSearchVersion.OPENDISTRO_1_9) >= 0) {
                arguments.add(
                        arguments("index-template", "_index_template",
                                TEST_INDEX_TEMPLATE_V1_FILE, TEST_INDEX_TEMPLATE_V2_FILE,
                                (BiFunction<Map<String, Object>, String, Integer>) (map, unused) ->
                                        (Integer) ((List<Map<String, Map<String, Object>>>) map.get("index_templates")).get(0).get("index_template").get("version")
                        )
                );
            }
            return arguments.stream();
        }
    }

    static class CreateWithIndexTemplateArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            final List<Arguments> arguments = new ArrayList<>();
            arguments.add(
                    arguments("v1", "_template",
                            TEST_TEMPLATE_V1_FILE,
                            (BiFunction<Map<String, Object>, String, Integer>) (map, templateName) ->
                                    (Integer) ((Map<String, Object>) map.get(templateName)).get("version"),
                            (BiFunction<Map<String, Object>, String, Map<String, Object>>) (map, templateName) ->
                                    (Map<String, Object>) ((Map<String, Object>) map.get(templateName)).get("mappings")
                    )
            );

            if (OpenSearchIntegrationHelper.getVersion().compareTo(DeclaredOpenSearchVersion.OPENDISTRO_1_9) >= 0) {
                arguments.add(
                        arguments("index-template", "_index_template",
                                TEST_INDEX_TEMPLATE_V1_FILE,
                                (BiFunction<Map<String, Object>, String, Integer>) (map, unused) ->
                                        (Integer) ((List<Map<String, Map<String, Object>>>) map.get("index_templates")).get(0).get("index_template").get("version"),
                                (BiFunction<Map<String, Object>, String, Map<String, Object>>) (map, templateName) ->
                                        (Map<String, Object>) ((List<Map<String, Map<String, Map<String, Object>>>>) map.get("index_templates")).get(0).get("index_template").get("template").get("mappings")
                        )
                );
            }
            return arguments.stream();
        }
    }

    static class CreateSingleWithTemplatesArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            final List<Arguments> arguments = new ArrayList<>();
            arguments.add(arguments("v1", TEST_TEMPLATE_V1_FILE));

            if (OpenSearchIntegrationHelper.getVersion().compareTo(DeclaredOpenSearchVersion.OPENDISTRO_1_9) >= 0) {
                arguments.add(arguments("index-template", TEST_INDEX_TEMPLATE_V1_FILE));
            }
            return arguments.stream();
        }
    }

    @Test
    public void testOutputCustomIndex() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final String testIdField = "someId";
        final String testId = "foo";
        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    @Test
    public void testOpenSearchBulkActionsCreate() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final String testIdField = "someId";
        final String testId = "foo";
        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        metadata.put(IndexConfiguration.ACTION, OpenSearchBulkActions.CREATE.toString());
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    @Test
    public void testOpenSearchBulkActionsCreateWithExpression() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final String testIdField = "someId";
        final String testId = "foo";
        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        Event event = (Event) testRecords.get(0).getData();
        event.getMetadata().setAttribute("action", "create");
        metadata.put(IndexConfiguration.ACTION, "create");
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    @Test
    public void testOpenSearchBulkActionsCreateWithInvalidExpression() throws IOException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final String testIdField = "someId";
        final String testId = "foo";
        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        metadata.put(IndexConfiguration.ACTION, "unknown");
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        assertThrows(NullPointerException.class, () -> createObjectUnderTest(openSearchSinkConfig, true));
    }

    @Test
    public void testBulkActionCreateWithActions() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_V1_FILE)).getFile();
        final String testIdField = "someId";
        final String testId = "foo";
        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.CREATE.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    @Test
    public void testBulkActionUpdateWithActions() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-upd1";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson2(testIdField, testId, "name", "value1")));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.CREATE.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
        testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson2(testIdField, testId, "name", "value2")));
        aList = new ArrayList<>();
        aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.UPDATE.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        retSources = getSearchResponseDocSources(testIndexAlias);

        assertThat(retSources.size(), equalTo(1));
        Map<String, Object> source = retSources.get(0);
        assertThat((String) source.get("name"), equalTo("value2"));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @Test
    public void testBulkActionUpdateWithDocumentRootKey() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-update";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        final String documentRootKey = "root_key";

        final String originalValue = "Original";
        final String updatedValue = "Updated";


        final String createJsonEvent = "{\"" + testIdField + "\": \"" + testId + "\", \"" + documentRootKey + "\": { \"value\": \"" + originalValue + "\"}}";

        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(createJsonEvent));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);

        metadata.put(IndexConfiguration.DOCUMENT_ROOT_KEY, documentRootKey);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put("type", OpenSearchBulkActions.CREATE.toString());


        aList.add(actionMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.get(0).containsKey(documentRootKey), equalTo(false));
        assertThat((String) retSources.get(0).get("value"), equalTo(originalValue));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);

        final String updateJsonEvent = "{\"" + testIdField + "\": \"" + testId + "\", \"" + documentRootKey + "\": { \"value\": \"" + updatedValue + "\"}}";

        testRecords = Collections.singletonList(jsonStringToRecord(updateJsonEvent));
        aList = new ArrayList<>();
        actionMap = new HashMap<>();
        actionMap.put("type", OpenSearchBulkActions.UPDATE.toString());
        aList.add(actionMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        retSources = getSearchResponseDocSources(testIndexAlias);

        assertThat(retSources.size(), equalTo(1));
        Map<String, Object> source = retSources.get(0);
        assertThat(source.containsKey(documentRootKey), equalTo(false));
        assertThat((String) source.get("value"), equalTo(updatedValue));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @Test
    public void testBulkActionUpsertWithActionsAndNoCreate() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-upsert-no-create2";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson2(testIdField, testId, "key", "value")));

        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put("type", OpenSearchBulkActions.UPSERT.toString());
        aList.add(actionMap);

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);

        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);

        assertThat(retSources.size(), equalTo(1));
        Map<String, Object> source = retSources.get(0);
        assertThat((String) source.get("key"), equalTo("value"));
        assertThat((String) source.get(testIdField), equalTo(testId));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @Test
    public void testBulkActionUpsertWithActions() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-upsert";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson2(testIdField, testId, "name", "value1")));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.CREATE.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);

        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
        testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson3(testIdField, testId, "name", "value3", "newKey", "newValue")));
        aList = new ArrayList<>();
        aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.UPSERT.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        retSources = getSearchResponseDocSources(testIndexAlias);

        assertThat(retSources.size(), equalTo(1));
        Map<String, Object> source = retSources.get(0);
        assertThat((String) source.get("name"), equalTo("value3"));
        assertThat((String) source.get("newKey"), equalTo("newValue"));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @Test
    public void testBulkActionUpsertWithoutCreate() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-upd2";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson3(testIdField, testId, "name", "value1", "newKey", "newValue")));
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.UPSERT.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);

        assertThat(retSources.size(), equalTo(1));
        Map<String, Object> source = retSources.get(0);
        assertThat((String) source.get("name"), equalTo("value1"));
        assertThat((String) source.get("newKey"), equalTo("newValue"));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();
        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    @Test
    public void testBulkActionDeleteWithActions() throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias-upd1";
        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource(TEST_TEMPLATE_BULK_FILE)).getFile();

        final String testIdField = "someId";
        final String testId = "foo";
        List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, testTemplateFile);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        List<Map<String, Object>> aList = new ArrayList<>();
        Map<String, Object> aMap = new HashMap<>();
        aMap.put("type", OpenSearchBulkActions.DELETE.toString());
        aList.add(aMap);
        metadata.put(IndexConfiguration.ACTIONS, aList);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(0));
        sink.shutdown();
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testEventOutputWithTags() throws IOException, InterruptedException {
        final Event testEvent = JacksonEvent.builder()
                .withData("{\"log\": \"foobar\"}")
                .withEventType("event")
                .build();
        List<String> tagsList = List.of("tag1", "tag2");
        testEvent.getMetadata().addTags(tagsList);

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTestWithSinkContext(openSearchSinkConfig, true);
        sink.output(testRecords);

        final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
        final Map<String, Object> expectedContent = new HashMap<>();
        expectedContent.put("log", "foobar");
        expectedContent.put(testTagsTargetKey, tagsList);

        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.containsAll(Arrays.asList(expectedContent)), equalTo(true));
        assertThat(getDocumentCount(expIndexAlias, "log", "foobar"), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @Test
    @DisabledIf(value = "isES6", disabledReason = TRACE_INGESTION_TEST_DISABLED_REASON)
    public void testEventOutput() throws IOException, InterruptedException {
        final String spanId = UUID.randomUUID().toString();
        final Event testEvent = JacksonEvent.builder()
                .withData("{\"log\": \"foobar\", \"spanId\": \""+spanId+"\"}")
                .withEventType("event")
                .build();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(IndexType.TRACE_ANALYTICS_RAW.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        verify(pluginConfigObservable).addPluginConfigObserver(any());
        sink.output(testRecords);

        final String expIndexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TRACE_ANALYTICS_RAW);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expIndexAlias);
        final Map<String, Object> expectedContent = new HashMap<>();
        expectedContent.put("log", "foobar");
        expectedContent.put("spanId", spanId);

        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.containsAll(Arrays.asList(expectedContent)), equalTo(true));
        assertThat(getDocumentCount(expIndexAlias, "log", "foobar"), equalTo(Integer.valueOf(1)));
        sink.shutdown();
    }

    @ParameterizedTest
    @MethodSource("getAttributeTestSpecialAndExtremeValues")
    public void testEventOutputWithSpecialAndExtremeValues(final Object testValue) throws IOException, InterruptedException {
        final String testIndexAlias = "test-alias";
        final String testField = "value";
        final Map<String, Object> data = new HashMap<>();
        data.put(testField, testValue);
        final Event testEvent = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        final Map<String, Object> expectedContent = new HashMap<>();
        expectedContent.put(testField, testValue);

        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources.get(0), equalTo(expectedContent));
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"info/ids/id", "id"})
    public void testOpenSearchDocumentId(final String testDocumentIdField) throws IOException, InterruptedException {
        final String expectedId = UUID.randomUUID().toString();
        final String testIndexAlias = "test_index";
        final Event testEvent = JacksonEvent.builder()
                .withData(Map.of("arbitrary_data", UUID.randomUUID().toString()))
                .withEventType("event")
                .build();
        testEvent.put(testDocumentIdField, expectedId);

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testDocumentIdField);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final List<String> docIds = getSearchResponseDocIds(testIndexAlias);
        for (String docId : docIds) {
            assertThat(docId, equalTo(expectedId));
        }
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"info/ids/rid", "rid"})
    public void testOpenSearchRoutingField(final String testRoutingField) throws IOException, InterruptedException {
        final String expectedRoutingField = UUID.randomUUID().toString();
        final String testIndexAlias = "test_index";
        final Event testEvent = JacksonEvent.builder()
                .withData(Map.of("arbitrary_data", UUID.randomUUID().toString()))
                .withEventType("event")
                .build();
        testEvent.put(testRoutingField, expectedRoutingField);

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        metadata.put(IndexConfiguration.ROUTING_FIELD, testRoutingField);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final List<String> routingFields = getSearchResponseRoutingFields(testIndexAlias);
        for (String routingField : routingFields) {
            assertThat(routingField, equalTo(expectedRoutingField));
        }
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "info/ids/rid", "rid"})
    public void testOpenSearchRouting(final String testRouting) throws IOException, InterruptedException {
        final String expectedRouting = UUID.randomUUID().toString();
        final String testIndexAlias = "test_index";
        final Event testEvent = JacksonEvent.builder()
                .withData(Map.of("arbitrary_data", UUID.randomUUID().toString()))
                .withEventType("event")
                .build();
        if (!testRouting.isEmpty()) {
            testEvent.put(testRouting, expectedRouting);
        }

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        if (!testRouting.isEmpty()) {
            metadata.put(IndexConfiguration.ROUTING, "${"+testRouting+"}");
        }
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final List<String> routingFields = getSearchResponseRoutingFields(testIndexAlias);
        for (String routingField : routingFields) {
            if (!testRouting.isEmpty()) {
                assertThat(routingField, equalTo(expectedRouting));
            } else {
                assertTrue(Objects.isNull(routingField));
            }
        }
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"info/ids/rid", "rid"})
    public void testOpenSearchRoutingWithExpressions(final String testRouting) throws IOException, InterruptedException {
        final String expectedRouting = UUID.randomUUID().toString();
        final String testIndexAlias = "test_index";
        final Event testEvent = JacksonEvent.builder()
                .withData(Map.of("arbitrary_data", UUID.randomUUID().toString()))
                .withEventType("event")
                .build();
        testEvent.put(testRouting, expectedRouting);

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        metadata.put(IndexConfiguration.ROUTING, "${/"+testRouting+"}");
        when(expressionEvaluator.isValidFormatExpression(any(String.class))).thenReturn(true);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);

        final List<String> routingFields = getSearchResponseRoutingFields(testIndexAlias);
        for (String routingField : routingFields) {
            assertThat(routingField, equalTo(expectedRouting));
        }
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"info/ids/rid", "rid"})
    public void testOpenSearchRoutingWithMixedExpressions(final String testRouting) throws IOException, InterruptedException {
        final String routing = UUID.randomUUID().toString();
        final String testIndexAlias = "test_index";
        final Event testEvent = JacksonEvent.builder()
                .withData(Map.of("arbitrary_data", UUID.randomUUID().toString()))
                .withEventType("event")
                .build();
        testEvent.put(testRouting, routing);

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final String prefix = RandomStringUtils.randomAlphabetic(5);
        final String suffix = RandomStringUtils.randomAlphabetic(6);
        Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        metadata.put(IndexConfiguration.ROUTING, prefix+"-${/"+testRouting+"}-"+suffix);
        when(expressionEvaluator.isValidFormatExpression(any(String.class))).thenReturn(true);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final String expectedRouting = prefix+"-"+routing+"-"+suffix;

        final List<String> routingFields = getSearchResponseRoutingFields(testIndexAlias);
        for (String field : routingFields) {
            assertThat(field, equalTo(expectedRouting));
        }
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"info/ids/id", "id"})
    public void testOpenSearchDynamicIndex(final String testIndex) throws IOException, InterruptedException {
        final String dynamicTestIndexAlias = "test-${" + testIndex + "}-index";
        final String testIndexName = "idx1";
        final String testIndexAlias = "test-" + testIndexName + "-index";
        final String data = UUID.randomUUID().toString();
        final Map<String, Object> dataMap = Map.of("data", data);
        final Event testEvent = JacksonEvent.builder()
                .withData(dataMap)
                .withEventType("event")
                .build();
        testEvent.put(testIndex, testIndexName);

        Map<String, Object> expectedMap = testEvent.toMap();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, dynamicTestIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources, hasItem(expectedMap));
        sink.shutdown();
    }

    @ParameterizedTest
    @CsvSource({
            "info/ids/id, yyyy-MM",
            "id, yyyy-MM-dd",
    })
    public void testOpenSearchDynamicIndexWithDate(final String testIndex, final String testDatePattern) throws IOException, InterruptedException {
        final String dynamicTestIndexAlias = "test-${" + testIndex + "}-index-%{" + testDatePattern + "}";
        final String testIndexName = "idx1";
        SimpleDateFormat formatter = new SimpleDateFormat(testDatePattern);
        Date date = new Date();
        String expectedDate = formatter.format(date);
        final String expectedIndexAlias = "test-" + testIndexName + "-index-" + expectedDate;
        final String data = UUID.randomUUID().toString();
        final Map<String, Object> dataMap = Map.of("data", data);
        final Event testEvent = JacksonEvent.builder()
                .withData(dataMap)
                .withEventType("event")
                .build();
        testEvent.put(testIndex, testIndexName);

        Map<String, Object> expectedMap = testEvent.toMap();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, dynamicTestIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expectedIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources, hasItem(expectedMap));
        sink.shutdown();
    }

    @ParameterizedTest
    @CsvSource({
            "id, yyyy-MM-dd, %{yyyy-MM-dd}-test-${id}-index",
            "id, yyyy-MM-dd, test-%{yyyy-MM-dd}-${id}-index",
            "id, yyyy-MM-dd, test-${id}-%{yyyy-MM-dd}-index",
    })
    public void testOpenSearchDynamicIndexWithDateNotAsSuffix(
            final String testIndex, final String testDatePattern, final String dynamicTestIndexAlias) throws IOException {
        final String testIndexName = "idx1";
        SimpleDateFormat formatter = new SimpleDateFormat(testDatePattern);
        Date date = new Date();
        String expectedDate = formatter.format(date);
        final String expectedIndexAlias = dynamicTestIndexAlias
                .replace("%{yyyy-MM-dd}", expectedDate)
                .replace("${id}", testIndexName);
        final String data = UUID.randomUUID().toString();
        final Map<String, Object> dataMap = Map.of("data", data);
        final Event testEvent = JacksonEvent.builder()
                .withData(dataMap)
                .withEventType("event")
                .build();
        testEvent.put(testIndex, testIndexName);

        Map<String, Object> expectedMap = testEvent.toMap();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, dynamicTestIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expectedIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources, hasItem(expectedMap));
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"yyyy-MM", "yyyy-MM-dd", "dd-MM-yyyy"})
    public void testOpenSearchIndexWithDate(final String testDatePattern) throws IOException, InterruptedException {
        SimpleDateFormat formatter = new SimpleDateFormat(testDatePattern);
        Date date = new Date();
        String expectedIndexName = "test-index-" + formatter.format(date);
        final String testIndexName = "idx1";
        final String testIndexAlias = "test-index-%{" + testDatePattern + "}";
        final String data = UUID.randomUUID().toString();
        final Map<String, Object> dataMap = Map.of("data", data);
        final Event testEvent = JacksonEvent.builder()
                .withData(dataMap)
                .withEventType("event")
                .build();

        Map<String, Object> expectedMap = testEvent.toMap();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expectedIndexName);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources, hasItem(expectedMap));
        sink.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"test-%{yyyy-MM-dd}-index", "%{yyyy-MM-dd}-test-index"})
    public void testOpenSearchIndexWithDateNotAsSuffix(final String testIndexAlias) throws IOException, InterruptedException {
        final String testDatePattern = "yyyy-MM-dd";
        SimpleDateFormat formatter = new SimpleDateFormat(testDatePattern);
        Date date = new Date();
        String expectedIndexName = testIndexAlias.replace("%{yyyy-MM-dd}", formatter.format(date));

        final String data = UUID.randomUUID().toString();
        final Map<String, Object> dataMap = Map.of("data", data);
        final Event testEvent = JacksonEvent.builder()
                .withData(dataMap)
                .withEventType("event")
                .build();

        Map<String, Object> expectedMap = testEvent.toMap();

        final List<Record<Event>> testRecords = Collections.singletonList(new Record<>(testEvent));

        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, testIndexAlias, null);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);
        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(expectedIndexName);
        assertThat(retSources.size(), equalTo(1));
        assertThat(retSources, hasItem(expectedMap));
        sink.shutdown();
    }

    @Test
    public void testOpenSearchIndexWithInvalidDate() throws IOException, InterruptedException {
        String invalidDatePattern = "yyyy-MM-dd HH:ss:mm";
        final String invalidTestIndexAlias = "test-index-%{" + invalidDatePattern + "}";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, invalidTestIndexAlias, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, false);
        Assert.assertThrows(IllegalArgumentException.class, () -> sink.doInitialize());
    }

    @Test
    public void testOpenSearchIndexWithInvalidChars() throws IOException, InterruptedException {
        final String invalidTestIndexAlias = "test#-index";
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfig(null, invalidTestIndexAlias, null);
        OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, false);
        Assert.assertThrows(RuntimeException.class, () -> sink.doInitialize());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    @DisabledIf(value = "isES6",
            disabledReason = "PUT _opendistro/_security/api/roles/<role-id> request could not be parsed in ES 6.")
    public void testOutputManagementDisabled() throws IOException, InterruptedException {
        final String testIndexAlias = "test-" + UUID.randomUUID();
        final String roleName = UUID.randomUUID().toString();
        final String username = UUID.randomUUID().toString();
        final String password = UUID.randomUUID().toString();
        final OpenSearchSecurityAccessor securityAccessor = new OpenSearchSecurityAccessor(client);
        securityAccessor.createBulkWritingRole(roleName, testIndexAlias + "*");
        securityAccessor.createUser(username, password, roleName);

        final String testIdField = "someId";
        final String testId = "foo";

        final List<Record<Event>> testRecords = Collections.singletonList(jsonStringToRecord(generateCustomRecordJson(testIdField, testId)));

        final Map<String, Object> metadata = initializeConfigurationMetadata(null, testIndexAlias, null);
        metadata.put(IndexConfiguration.INDEX_TYPE, IndexType.MANAGEMENT_DISABLED.getValue());
        metadata.put(USERNAME, username);
        metadata.put(PASSWORD, password);
        metadata.put(IndexConfiguration.DOCUMENT_ID_FIELD, testIdField);
        final OpenSearchSinkConfig openSearchSinkConfig = generateOpenSearchSinkConfigByMetadata(metadata);
        final OpenSearchSink sink = createObjectUnderTest(openSearchSinkConfig, true);

        final String testTemplateFile = Objects.requireNonNull(
                getClass().getClassLoader().getResource("management-disabled-index-template.json")).getFile();
        createV1IndexTemplate(testIndexAlias, testIndexAlias + "*", testTemplateFile);
        createIndex(testIndexAlias);

        sink.output(testRecords);
        final List<Map<String, Object>> retSources = getSearchResponseDocSources(testIndexAlias);
        assertThat(retSources.size(), equalTo(1));
        assertThat(getDocumentCount(testIndexAlias, "_id", testId), equalTo(Integer.valueOf(1)));
        sink.shutdown();

        // verify metrics
        final List<Measurement> bulkRequestLatencies = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_LATENCY).toString());
        assertThat(bulkRequestLatencies.size(), equalTo(3));
        // COUNT
        Assert.assertEquals(1.0, bulkRequestLatencies.get(0).getValue(), 0);
    }

    private Map<String, Object> initializeConfigurationMetadata(final String indexType, final String indexAlias,
                                                                final String templateFilePath) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, indexType);
        metadata.put(ConnectionConfiguration.HOSTS, getHosts());
        metadata.put(IndexConfiguration.INDEX_ALIAS, indexAlias);
        metadata.put(IndexConfiguration.TEMPLATE_FILE, templateFilePath);
        metadata.put(IndexConfiguration.FLUSH_TIMEOUT, -1);
        final String user = System.getProperty("tests.opensearch.user");
        final String password = System.getProperty("tests.opensearch.password");
        if (user != null) {
            metadata.put(AUTHENTICATION, Map.of(USERNAME, user, PASSWORD, password));
        }
        final String distributionVersion = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ?
                DistributionVersion.ES6.getVersion() : DistributionVersion.DEFAULT.getVersion();
        metadata.put(IndexConfiguration.DISTRIBUTION_VERSION, distributionVersion);
        return metadata;
    }

    private Map<String, Object> initializeConfigurationMetadata(final String indexType, final String indexAlias,
                                                              final String templateFilePath, final boolean estimateBulkSizeUsingCompression,
                                                              final boolean requestCompressionEnabled) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigurationMetadata(indexType, indexAlias, templateFilePath);
        metadata.put(IndexConfiguration.ESTIMATE_BULK_SIZE_USING_COMPRESSION, estimateBulkSizeUsingCompression);
        metadata.put(ConnectionConfiguration.REQUEST_COMPRESSION_ENABLED, requestCompressionEnabled);
        return metadata;
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(final String indexType, final String indexAlias,
                                                final String templateFilePath) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigurationMetadata(indexType, indexAlias, templateFilePath);
        return generateOpenSearchSinkConfigByMetadata(metadata);
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(final String indexType, final String indexAlias,
                                                final String templateFilePath, final boolean estimateBulkSizeUsingCompression,
                                                final boolean requestCompressionEnabled) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigurationMetadata(indexType, indexAlias, templateFilePath);
        metadata.put(IndexConfiguration.ESTIMATE_BULK_SIZE_USING_COMPRESSION, estimateBulkSizeUsingCompression);
        metadata.put(ConnectionConfiguration.REQUEST_COMPRESSION_ENABLED, requestCompressionEnabled);
        return generateOpenSearchSinkConfigByMetadata(metadata);
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(final String indexType, final String indexAlias,
                                                final String templateType,
                                                final String templateFilePath) throws JsonProcessingException {
        final Map<String, Object> metadata = initializeConfigurationMetadata(indexType, indexAlias, templateFilePath);
        metadata.put(IndexConfiguration.TEMPLATE_TYPE, templateType);
        return generateOpenSearchSinkConfigByMetadata(metadata);
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfigByMetadata(final Map<String, Object> configurationMetadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        String json = new ObjectMapper().writeValueAsString(configurationMetadata);
        OpenSearchSinkConfig openSearchSinkConfig = objectMapper.readValue(json, OpenSearchSinkConfig.class);

        return openSearchSinkConfig;
    }

    private String generateCustomRecordJson(final String idField, final String documentId) throws IOException {
        return Strings.toString(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field(idField, documentId)
                        .endObject()
        );
    }

    private String generateCustomRecordJson2(final String idField, final String documentId, final String key, final String value) throws IOException {
        return Strings.toString(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field(idField, documentId)
                        .field(key, value)
                        .endObject()
        );
    }

    private String generateCustomRecordJson3(final String idField, final String documentId, final String key1, final String value1, final String key2, final String value2) throws IOException {
        return Strings.toString(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field(idField, documentId)
                        .field(key1, value1)
                        .field(key2, value2)
                        .endObject()
        );
    }

    private String readDocFromFile(final String filename) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(filename))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    private Boolean checkIsWriteIndex(final String responseBody, final String aliasName, final String indexName) throws IOException {
        @SuppressWarnings("unchecked") final Map<String, Object> indexBlob = (Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                responseBody).map().get(indexName);
        @SuppressWarnings("unchecked") final Map<String, Object> aliasesBlob = (Map<String, Object>) indexBlob.get("aliases");
        @SuppressWarnings("unchecked") final Map<String, Object> aliasBlob = (Map<String, Object>) aliasesBlob.get(aliasName);
        return (Boolean) aliasBlob.get("is_write_index");
    }

    private Integer getDocumentCount(final String index, final String field, final String value) throws IOException, InterruptedException {
        final Request request = new Request(HttpMethod.GET, index + "/_count");
        if (field != null && value != null) {
            final String jsonEntity = Strings.toString(
                    XContentFactory.jsonBuilder().startObject()
                            .startObject("query")
                            .startObject("match")
                            .field(field, value)
                            .endObject()
                            .endObject()
                            .endObject()
            );
            request.setJsonEntity(jsonEntity);
        }
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());
        return (Integer) createContentParser(XContentType.JSON.xContent(), responseBody).map().get("count");
    }

    private List<String> getSearchResponseDocIds(final String index) throws IOException {
        final Request refresh = new Request(HttpMethod.POST, index + "/_refresh");
        client.performRequest(refresh);
        final Request request = new Request(HttpMethod.GET, index + "/_search");
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked") final List<Object> hits =
                (List<Object>) ((Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                        responseBody).map().get("hits")).get("hits");
        @SuppressWarnings("unchecked") final List<String> ids = hits.stream()
                .map(hit -> (String) ((Map<String, Object>) hit).get("_id"))
                .collect(Collectors.toList());
        return ids;
    }

    private List<String> getSearchResponseRoutingFields(final String index) throws IOException {
        final Request refresh = new Request(HttpMethod.POST, index + "/_refresh");
        client.performRequest(refresh);
        final Request request = new Request(HttpMethod.GET, index + "/_search");
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked") final List<Object> hits =
                (List<Object>) ((Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                        responseBody).map().get("hits")).get("hits");
        @SuppressWarnings("unchecked") final List<String> routingFields = hits.stream()
                .map(hit -> (String) ((Map<String, Object>) hit).get("_routing"))
                .collect(Collectors.toList());
        return routingFields;
    }

    private List<Map<String, Object>> getSearchResponseDocSources(final String index) throws IOException {
        final Request refresh = new Request(HttpMethod.POST, index + "/_refresh");
        client.performRequest(refresh);
        final Request request = new Request(HttpMethod.GET, index + "/_search");
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked") final List<Object> hits =
                (List<Object>) ((Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                        responseBody).map().get("hits")).get("hits");
        @SuppressWarnings("unchecked") final List<Map<String, Object>> sources = hits.stream()
                .map(hit -> (Map<String, Object>) ((Map<String, Object>) hit).get("_source"))
                .collect(Collectors.toList());
        return sources;
    }

    private Map<String, Object> getIndexMappings(final String index) throws IOException {
        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        final Request request = new Request(HttpMethod.GET, index + "/_mappings" + extraURI);
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked") final Map<String, Object> mappings =
                (Map<String, Object>) ((Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                        responseBody).map().get(index)).get("mappings");
        return mappings;
    }

    private JsonNode getIndexSettings(final String index) throws IOException {
        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        final Request request = new Request(HttpMethod.GET, index + "/_settings" + extraURI);
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        Map<String, Object> responseMap = createContentParser(XContentType.JSON.xContent(), responseBody).map();

        return new ObjectMapper().convertValue(responseMap, JsonNode.class)
                .get(index)
                .get("settings");
    }

    private String getIndexPolicyId(final String index) throws IOException {
        // TODO: replace with new _opensearch API
        final Request request = new Request(HttpMethod.GET, "/_opendistro/_ism/explain/" + index);
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked") final String policyId = (String) ((Map<String, Object>) createContentParser(XContentType.JSON.xContent(),
                responseBody).map().get(index)).get("index.opendistro.index_state_management.policy_id");
        return policyId;
    }


    @SuppressWarnings("unchecked")
    private void wipeAllOpenSearchIndices() throws IOException {
        final String getIndicesEndpoint = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ?
                "/*?expand_wildcards=all&include_type_name=false" : "/*?expand_wildcards=all";
        final Response response = client.performRequest(new Request("GET", getIndicesEndpoint));

        final String responseBody = EntityUtils.toString(response.getEntity());
        final Map<String, Object> indexContent = createContentParser(XContentType.JSON.xContent(), responseBody).map();

        final Set<String> indices = indexContent.keySet();

        indices.stream()
                .filter(Objects::nonNull)
                .filter(Predicate.not(indexName -> indexName.startsWith(".opendistro-")))
                .filter(Predicate.not(indexName -> indexName.startsWith(".opendistro_")))
                .filter(Predicate.not(indexName -> indexName.startsWith(".opensearch-")))
                .filter(Predicate.not(indexName -> indexName.startsWith(".opensearch_")))
                .filter(Predicate.not(indexName -> indexName.startsWith(".ql")))
                .filter(Predicate.not(indexName -> indexName.startsWith(".plugins-ml-config")))
                .forEach(indexName -> {
                    try {
                        client.performRequest(new Request("DELETE", "/" + indexName));
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private Record jsonStringToRecord(final String jsonString) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            Record record = new Record(JacksonEvent.builder()
                    .withEventType(EventType.TRACE.toString())
                    .withData(objectMapper.readValue(jsonString, Map.class)).build());
            JacksonEvent event = (JacksonEvent) record.getData();
            return record;
        } catch (final JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void createIndex(final String indexName) throws IOException {
        final Request request = new Request(HttpMethod.PUT, indexName);
        final Response response = client.performRequest(request);
    }

    private void createV1IndexTemplate(final String templateName, final String indexPattern, final String fileName) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final Map<String, Object> templateJson = objectMapper.readValue(new FileInputStream(fileName), Map.class);

        templateJson.put("index_patterns", indexPattern);

        final Request request = new Request(HttpMethod.PUT, "_template/" + templateName);

        final String createTemplateJson = objectMapper.writeValueAsString(templateJson);
        request.setJsonEntity(createTemplateJson);
        final Response response = client.performRequest(request);
    }

    private static boolean isES6() {
        return DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(OpenSearchIntegrationHelper.getVersion()) >= 0;
    }

    private static Stream<Object> getAttributeTestSpecialAndExtremeValues() {
        return Stream.of(
                null,
                Arguments.of(Long.MAX_VALUE),
                Arguments.of(Long.MIN_VALUE),
                Arguments.of(Integer.MAX_VALUE),
                Arguments.of(Integer.MIN_VALUE),
                Arguments.of(RandomStringUtils.randomAlphabetic(LUCENE_CHAR_LENGTH_LIMIT)),
                Arguments.of(TEST_STRING_WITH_SPECIAL_CHARS),
                Arguments.of(TEST_STRING_WITH_NON_LATIN_CHARS),
                Arguments.of(Double.MIN_VALUE),
                Arguments.of(-Double.MIN_VALUE),
                Arguments.of((double) Float.MAX_VALUE),
                Arguments.of((double) Float.MIN_VALUE),
                Arguments.of((double) -Float.MAX_VALUE),
                Arguments.of((double) -Float.MIN_VALUE),
                Arguments.of(Boolean.TRUE),
                Arguments.of(Boolean.FALSE)
                );
    }
}
