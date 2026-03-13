/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Measurement;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledIf;
import org.mockito.Mock;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConstants;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.http.HttpStatus.SC_OK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createContentParser;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createOpenSearchClient;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.getHosts;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.waitForClusterStateUpdatesToFinish;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.wipeAllTemplates;

/**
 * Integration tests for the OpenSearch sink with {@code index_type: tsdb}.
 *
 * <p>These tests require a running OpenSearch cluster. Configure via system properties:
 * <ul>
 *   <li>{@code tests.opensearch.host}</li>
 *   <li>{@code tests.opensearch.user} (optional)</li>
 *   <li>{@code tests.opensearch.password} (optional)</li>
 * </ul>
 *
 * <p>Note: TSDB-specific settings ({@code tsdb_engine.enabled}, {@code tsdb_store}) require the TSDB plugin
 * installed on the OpenSearch cluster. These tests validate the sink's document conversion and indexing
 * behavior using the standard TSDB index template. If the TSDB plugin is not installed, index creation
 * will use the template mappings but TSDB-specific settings will be ignored by OpenSearch.
 */
class OpenSearchSinkTsdbIT {

    private static final String AUTHENTICATION = "authentication";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "tsdbIntegTestPipeline";
    private static final String INCLUDE_TYPE_NAME_FALSE_URI = "?include_type_name=false";
    private static final String TEST_TIME = "2024-02-02T10:30:00Z";

    private RestClient client;
    private final List<OpenSearchSink> sinksToShutdown = new ArrayList<>();
    private ObjectMapper objectMapper;

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

    @BeforeEach
    void setup() {
        pluginConfigObservable = mock(PluginConfigObservable.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginSetting = mock(PluginSetting.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        when(expressionEvaluator.isValidExpressionStatement(any(String.class))).thenReturn(false);
        objectMapper = new ObjectMapper();
    }

    @BeforeEach
    void metricsInit() throws IOException {
        MetricsTestUtil.initMetrics();
        client = createOpenSearchClient();
    }

    @AfterEach
    void cleanOpenSearch() throws Exception {
        wipeAllOpenSearchIndices();
        wipeAllTemplates();
        waitForClusterStateUpdatesToFinish();
    }

    @AfterEach
    void shutdownSinks() {
        for (final OpenSearchSink sink : sinksToShutdown) {
            sink.shutdown();
        }
        sinksToShutdown.clear();
    }

    @AfterEach
    void closeClient() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    // --- Index Initialization ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testInstantiateSinkTsdbDefault() throws IOException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        assertThat(indexAlias, equalTo("metrics-tsdb-v1"));

        final Request request = new Request(HttpMethod.HEAD, indexAlias);
        final Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Query mappings via alias (TSDB uses NoIsmPolicyManagement so index name may not follow -000001 pattern)
        final String extraURI = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ? INCLUDE_TYPE_NAME_FALSE_URI : "";
        final Request mappingRequest = new Request(HttpMethod.GET, indexAlias + "/_mappings" + extraURI);
        final Response mappingResponse = client.performRequest(mappingRequest);
        final String mappingBody = EntityUtils.toString(mappingResponse.getEntity());
        @SuppressWarnings("unchecked")
        final Map<String, Object> mappingResult = createContentParser(XContentType.JSON.xContent(), mappingBody).map();
        final String actualIndex = mappingResult.keySet().iterator().next();
        @SuppressWarnings("unchecked")
        final Map<String, Object> mappings = (Map<String, Object>) ((Map<String, Object>) mappingResult.get(actualIndex)).get("mappings");
        assertThat(mappings, notNullValue());

        // Verify TSDB-specific mapping fields
        @SuppressWarnings("unchecked")
        final Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat(properties, notNullValue());
        assertThat(properties.containsKey("labels"), equalTo(true));
        assertThat(properties.containsKey("timestamp"), equalTo(true));
        assertThat(properties.containsKey("value"), equalTo(true));

        // TSDB uses NoIsmPolicyManagement — no ISM policy should be attached
        // (unlike metric-analytics which has ISM)
    }

    // --- Gauge Output ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testOutputGauge() throws IOException, InterruptedException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("cpu_temp")
                .withValue(72.5)
                .withTime(TEST_TIME)
                .withAttributes(Map.of("host", "server-01"))
                .withEventKind("GAUGE")
                .build();

        sink.output(List.of(new Record<>(gauge)));

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        final List<Map<String, Object>> sources = getSearchResponseDocSources(indexAlias);
        assertThat(sources, hasSize(1));

        final Map<String, Object> doc = sources.get(0);
        assertThat(doc.get("labels"), equalTo("__name__ cpu_temp host server-01"));
        assertThat(((Number) doc.get("value")).doubleValue(), closeTo(72.5, 0.001));
        assertThat(doc.get("timestamp"), notNullValue());

        // Verify metrics
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
    }

    // --- Sum (Counter) Output ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testOutputMonotonicSum() throws IOException, InterruptedException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final JacksonSum sum = JacksonSum.builder()
                .withName("http_requests")
                .withValue(100.0)
                .withIsMonotonic(true)
                .withTime(TEST_TIME)
                .withAttributes(Map.of("method", "GET"))
                .withEventKind("SUM")
                .build();

        sink.output(List.of(new Record<>(sum)));

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        final List<Map<String, Object>> sources = getSearchResponseDocSources(indexAlias);
        assertThat(sources, hasSize(1));

        final Map<String, Object> doc = sources.get(0);
        // Monotonic sum should have _total suffix
        assertThat(doc.get("labels"), equalTo("__name__ http_requests_total method GET"));
        assertThat(((Number) doc.get("value")).doubleValue(), closeTo(100.0, 0.001));

        // Verify metrics
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
    }

    // --- Histogram Expansion ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testOutputHistogramExpansion() throws IOException, InterruptedException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final JacksonHistogram histogram = JacksonHistogram.builder()
                .withName("request_duration")
                .withSum(5.5)
                .withCount(20L)
                .withBucketCountsList(List.of(5L, 5L, 5L, 5L))
                .withExplicitBoundsList(List.of(0.1, 0.5, 1.0))
                .withTime(TEST_TIME)
                .withAttributes(Map.of("method", "GET"))
                .withEventKind("HISTOGRAM")
                .build();

        sink.output(List.of(new Record<>(histogram)));

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        final List<Map<String, Object>> sources = getSearchResponseDocSources(indexAlias);
        // 4 bucket docs + 1 _count + 1 _sum = 6
        assertThat(sources, hasSize(6));

        // Collect labels for verification
        final List<String> labels = sources.stream()
                .map(s -> (String) s.get("labels"))
                .collect(Collectors.toList());

        // Verify bucket docs exist with cumulative counts
        assertThat(labels.stream().filter(l -> l.contains("request_duration_bucket")).count(), equalTo(4L));
        assertThat(labels.stream().filter(l -> l.contains("request_duration_count")).count(), equalTo(1L));
        assertThat(labels.stream().filter(l -> l.contains("request_duration_sum")).count(), equalTo(1L));

        // Verify cumulative bucket values
        final Map<String, Double> labelToValue = sources.stream()
                .collect(Collectors.toMap(s -> (String) s.get("labels"), s -> ((Number) s.get("value")).doubleValue()));

        assertThat(labelToValue.get("__name__ request_duration_bucket le 0.1 method GET"), closeTo(5.0, 0.001));
        assertThat(labelToValue.get("__name__ request_duration_bucket le 0.5 method GET"), closeTo(10.0, 0.001));
        assertThat(labelToValue.get("__name__ request_duration_bucket le 1 method GET"), closeTo(15.0, 0.001));
        assertThat(labelToValue.get("__name__ request_duration_bucket le +Inf method GET"), closeTo(20.0, 0.001));
        assertThat(labelToValue.get("__name__ request_duration_count method GET"), closeTo(20.0, 0.001));
        assertThat(labelToValue.get("__name__ request_duration_sum method GET"), closeTo(5.5, 0.001));

        // Verify document success count matches expanded document count
        final List<Measurement> documentsSuccess = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(BulkRetryStrategy.DOCUMENTS_SUCCESS).toString());
        assertThat(documentsSuccess.size(), equalTo(1));
        assertThat(documentsSuccess.get(0).getValue(), closeTo(6.0, 0));

        // Verify no errors
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
    }

    // --- Summary Expansion ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testOutputSummaryExpansion() throws IOException, InterruptedException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final List<Quantile> quantiles = Arrays.asList(
                new DefaultQuantile(0.5, 0.2),
                new DefaultQuantile(0.99, 0.8)
        );
        final JacksonSummary summary = JacksonSummary.builder()
                .withName("rpc_latency")
                .withQuantiles(quantiles)
                .withQuantilesValueCount(2)
                .withCount(1000L)
                .withSum(300.5)
                .withTime(TEST_TIME)
                .withAttributes(Map.of("service", "api"))
                .withEventKind("SUMMARY")
                .build();

        sink.output(List.of(new Record<>(summary)));

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        final List<Map<String, Object>> sources = getSearchResponseDocSources(indexAlias);
        // 2 quantile docs + 1 _count + 1 _sum = 4
        assertThat(sources, hasSize(4));

        final Map<String, Double> labelToValue = sources.stream()
                .collect(Collectors.toMap(s -> (String) s.get("labels"), s -> ((Number) s.get("value")).doubleValue()));

        assertThat(labelToValue.get("__name__ rpc_latency quantile 0.5 service api"), closeTo(0.2, 0.001));
        assertThat(labelToValue.get("__name__ rpc_latency quantile 0.99 service api"), closeTo(0.8, 0.001));
        assertThat(labelToValue.get("__name__ rpc_latency_count service api"), closeTo(1000.0, 0.001));
        assertThat(labelToValue.get("__name__ rpc_latency_sum service api"), closeTo(300.5, 0.001));

        // Verify metrics
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
    }

    // --- Multiple Metrics in Single Batch ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testOutputMixedMetricTypes() throws IOException, InterruptedException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        final OpenSearchSink sink = createObjectUnderTest(config, true);

        final JacksonGauge gauge = JacksonGauge.builder()
                .withName("cpu_temp")
                .withValue(72.5)
                .withTime(TEST_TIME)
                .withAttributes(Map.of("host", "server-01"))
                .withEventKind("GAUGE")
                .build();

        final JacksonSum counter = JacksonSum.builder()
                .withName("requests")
                .withValue(500.0)
                .withIsMonotonic(true)
                .withTime(TEST_TIME)
                .withAttributes(Map.of("path", "/api"))
                .withEventKind("SUM")
                .build();

        final List<Record<Event>> records = List.of(new Record<>(gauge), new Record<>(counter));
        sink.output(records);

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        final List<Map<String, Object>> sources = getSearchResponseDocSources(indexAlias);
        // 1 gauge + 1 sum = 2 documents
        assertThat(sources, hasSize(2));

        // Verify metrics
        final List<Measurement> bulkRequestErrors = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(PIPELINE_NAME).add(PLUGIN_NAME)
                        .add(OpenSearchSink.BULKREQUEST_ERRORS).toString());
        assertThat(bulkRequestErrors.size(), equalTo(1));
        Assert.assertEquals(0.0, bulkRequestErrors.get(0).getValue(), 0);
    }

    // --- Re-instantiation (no duplicate index) ---

    @Test
    @DisabledIf(value = "isES6", disabledReason = "TSDB is not supported for ES 6")
    @Timeout(value = 50, unit = TimeUnit.SECONDS)
    void testReinstantiateSinkDoesNotCreateDuplicateIndex() throws IOException {
        final OpenSearchSinkConfig config = generateOpenSearchSinkConfig(IndexType.TSDB.getValue(), null, null);
        createObjectUnderTest(config, true);

        final String indexAlias = IndexConstants.TYPE_TO_DEFAULT_ALIAS.get(IndexType.TSDB);
        Request request = new Request(HttpMethod.HEAD, indexAlias);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));

        // Reinstantiate sink — should not fail
        createObjectUnderTest(config, true);

        request = new Request(HttpMethod.HEAD, indexAlias);
        response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(SC_OK));
    }

    // --- Helper methods ---

    private OpenSearchSink createObjectUnderTest(final OpenSearchSinkConfig openSearchSinkConfig, final boolean doInitialize) {
        final SinkContext sinkContext = mock(SinkContext.class);
        when(sinkContext.getTagsTargetKey()).thenReturn(null);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of());
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);
        final OpenSearchSink sink = new OpenSearchSink(
                pluginSetting, sinkContext, expressionEvaluator, awsCredentialsSupplier,
                pipelineDescription, pluginConfigObservable, openSearchSinkConfig);
        if (doInitialize) {
            sink.doInitialize();
        }
        sinksToShutdown.add(sink);
        return sink;
    }

    private OpenSearchSinkConfig generateOpenSearchSinkConfig(final String indexType, final String indexAlias,
                                                              final String templateFilePath) throws JsonProcessingException {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(IndexConfiguration.INDEX_TYPE, indexType);
        metadata.put(ConnectionConfiguration.HOSTS, getHosts());
        metadata.put(IndexConfiguration.INDEX_ALIAS, indexAlias);
        metadata.put(IndexConfiguration.TEMPLATE_FILE, templateFilePath);
        metadata.put(IndexConfiguration.FLUSH_TIMEOUT, -1);
        metadata.put("insecure", true);
        final String user = System.getProperty("tests.opensearch.user");
        final String password = System.getProperty("tests.opensearch.password");
        if (user != null) {
            metadata.put(AUTHENTICATION, Map.of(USERNAME, user, PASSWORD, password));
        }
        final String distributionVersion = DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(
                OpenSearchIntegrationHelper.getVersion()) >= 0 ?
                DistributionVersion.ES6.getVersion() : DistributionVersion.DEFAULT.getVersion();
        metadata.put(IndexConfiguration.DISTRIBUTION_VERSION, distributionVersion);

        final String json = new ObjectMapper().writeValueAsString(metadata);
        return objectMapper.readValue(json, OpenSearchSinkConfig.class);
    }

    private List<Map<String, Object>> getSearchResponseDocSources(final String index) throws IOException {
        final Request refresh = new Request(HttpMethod.POST, index + "/_refresh");
        client.performRequest(refresh);
        final Request request = new Request(HttpMethod.GET, index + "/_search");
        final Response response = client.performRequest(request);
        final String responseBody = EntityUtils.toString(response.getEntity());

        @SuppressWarnings("unchecked")
        final List<Object> hits = (List<Object>) ((Map<String, Object>) createContentParser(
                XContentType.JSON.xContent(), responseBody).map().get("hits")).get("hits");
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> sources = hits.stream()
                .map(hit -> (Map<String, Object>) ((Map<String, Object>) hit).get("_source"))
                .collect(Collectors.toList());
        return sources;
    }

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

    private static boolean isES6() {
        return DeclaredOpenSearchVersion.OPENDISTRO_0_10.compareTo(OpenSearchIntegrationHelper.getVersion()) >= 0;
    }
}
