/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;

import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createContentParser;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.createOpenSearchClient;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchIntegrationHelper.getHosts;

@EnabledIfSystemProperty(named = "tests.mtls.client.cert", matches = ".+")
@EnabledIfSystemProperty(named = "tests.mtls.client.key", matches = ".+")
class OpenSearchClientCertIT {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String PLUGIN_NAME = "opensearch";
    private static final String PIPELINE_NAME = "mtlsIntegTestPipeline";

    private RestClient client;
    private ObjectMapper objectMapper;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private ExpressionEvaluator expressionEvaluator;
    private PipelineDescription pipelineDescription;
    private PluginSetting pluginSetting;
    private PluginConfigObservable pluginConfigObservable;
    private OpenSearchSink sink;

    @BeforeEach
    void setUp() throws IOException {
        MetricsTestUtil.initMetrics();

        objectMapper = new ObjectMapper();
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        pipelineDescription = mock(PipelineDescription.class);
        pluginSetting = mock(PluginSetting.class);
        pluginConfigObservable = mock(PluginConfigObservable.class);

        when(expressionEvaluator.isValidExpressionStatement(any(String.class))).thenReturn(false);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getPipelineName()).thenReturn(PIPELINE_NAME);
        when(pluginSetting.getName()).thenReturn(PLUGIN_NAME);

        client = createOpenSearchClient();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (sink != null) {
            sink.shutdown();
        }
        if (client != null) {
            client.close();
        }
    }

    @Test
    void sink_with_mtls_can_index_documents_and_verify_on_cluster() throws IOException, InterruptedException {
        final String testIndex = "mtls-it-index-" + UUID.randomUUID().toString().substring(0, 8);

        final Map<String, Object> metadata = createConfigurationMetadata(testIndex);
        metadata.put("authentication", Map.of(
                USERNAME, System.getProperty("tests.opensearch.user", "admin"),
                PASSWORD, System.getProperty("tests.opensearch.password", "admin"),
                "client_certificate", System.getProperty("tests.mtls.client.cert"),
                "client_key", System.getProperty("tests.mtls.client.key")));

        final OpenSearchSinkConfig openSearchSinkConfig = createOpenSearchSinkConfig(metadata);
        sink = createSink(openSearchSinkConfig);

        final String docId = UUID.randomUUID().toString();
        final Map<String, Object> eventData = Map.of("message", "mTLS integration test", "docId", docId);
        final Event event = JacksonEvent.builder()
                .withEventType(EventType.LOG.toString())
                .withData(eventData)
                .build();
        final List<Record<Event>> records = Collections.singletonList(new Record<>(event));

        sink.output(records);

        final List<Map<String, Object>> sources = getSearchResponseDocSources(testIndex);
        assertThat(sources.size(), greaterThanOrEqualTo(1));
        assertThat(sources.get(0).get("message"), equalTo("mTLS integration test"));
        assertThat(sources.get(0).get("docId"), equalTo(docId));

        deleteIndex(testIndex);
    }

    private Map<String, Object> createConfigurationMetadata(final String indexAlias) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put(ConnectionConfiguration.HOSTS, getHosts());
        metadata.put(IndexConfiguration.INDEX_ALIAS, indexAlias);
        metadata.put(IndexConfiguration.FLUSH_TIMEOUT, -1);
        metadata.put("insecure", true);
        return metadata;
    }

    private OpenSearchSinkConfig createOpenSearchSinkConfig(final Map<String, Object> metadata) throws JsonProcessingException {
        final String json = objectMapper.writeValueAsString(metadata);
        return objectMapper.readValue(json, OpenSearchSinkConfig.class);
    }

    private OpenSearchSink createSink(final OpenSearchSinkConfig config) {
        final SinkContext sinkContext = mock(SinkContext.class);
        when(sinkContext.getTagsTargetKey()).thenReturn(null);
        when(sinkContext.getForwardToPipelines()).thenReturn(Map.of());
        final OpenSearchSink openSearchSink = new OpenSearchSink(
                pluginSetting, sinkContext, expressionEvaluator, awsCredentialsSupplier,
                pipelineDescription, pluginConfigObservable, config);
        openSearchSink.doInitialize();
        return openSearchSink;
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

    private void deleteIndex(final String index) throws IOException {
        final Request request = new Request(HttpMethod.DELETE, index);
        client.performRequest(request);
    }
}
