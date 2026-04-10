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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import jakarta.json.stream.JsonGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ScriptConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexConfiguration;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManagerFactory;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexType;
import org.opensearch.dataprepper.plugins.sink.opensearch.index.TemplateType;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.EXTERNAL_LATENCY;
import static org.opensearch.dataprepper.model.sink.SinkLatencyMetrics.INTERNAL_LATENCY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_LATENCY;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.BULKREQUEST_SIZE_BYTES;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.DYNAMIC_INDEX_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_ACTION_ERRORS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSink.INVALID_VERSION_EXPRESSION_DROPPED_EVENTS;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_BULK_SIZE;
import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_FLUSH_TIMEOUT;

@ExtendWith(MockitoExtension.class)
public class OpenSearchSinkScriptTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();

    @Mock private IndexManagerFactory indexManagerFactory;
    @Mock private OpenSearchClient openSearchClient;
    @Mock private SinkContext sinkContext;
    @Mock private PluginSetting pluginSetting;
    @Mock private ExpressionEvaluator expressionEvaluator;
    @Mock private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock private OpenSearchSinkConfiguration openSearchSinkConfiguration;
    @Mock private PipelineDescription pipelineDescription;
    @Mock private IndexConfiguration indexConfiguration;
    @Mock private PluginMetrics pluginMetrics;
    @Mock private OpenSearchSinkConfig openSearchSinkConfig;
    @Mock private PluginConfigObservable pluginConfigObservable;
    @Mock private ScriptConfiguration scriptConfiguration;

    @BeforeEach
    void setup() {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());

        final RetryConfiguration retryConfiguration = mock(RetryConfiguration.class);
        when(retryConfiguration.getDlq()).thenReturn(Optional.empty());
        lenient().when(retryConfiguration.getDlqFile()).thenReturn(null);

        final ConnectionConfiguration connectionConfiguration = mock(ConnectionConfiguration.class);
        final RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
        lenient().when(connectionConfiguration.createClient(awsCredentialsSupplier)).thenReturn(restHighLevelClient);
        lenient().when(connectionConfiguration.createOpenSearchClient(restHighLevelClient, awsCredentialsSupplier)).thenReturn(openSearchClient);

        when(indexConfiguration.getDocumentId()).thenReturn(null);
        when(indexConfiguration.getDocumentIdField()).thenReturn(null);
        when(indexConfiguration.getRouting()).thenReturn(null);
        when(indexConfiguration.getActions()).thenReturn(null);
        when(indexConfiguration.getDocumentRootKey()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionType()).thenReturn(null);
        lenient().when(indexConfiguration.getVersionExpression()).thenReturn(null);
        lenient().when(indexConfiguration.getIndexAlias()).thenReturn(UUID.randomUUID().toString());
        lenient().when(indexConfiguration.getTemplateType()).thenReturn(TemplateType.V1);
        when(indexConfiguration.getIndexType()).thenReturn(IndexType.CUSTOM);
        when(indexConfiguration.getBulkSize()).thenReturn(DEFAULT_BULK_SIZE);
        when(indexConfiguration.getFlushTimeout()).thenReturn(DEFAULT_FLUSH_TIMEOUT);

        when(openSearchSinkConfiguration.getIndexConfiguration()).thenReturn(indexConfiguration);
        when(openSearchSinkConfiguration.getRetryConfiguration()).thenReturn(retryConfiguration);
        lenient().when(openSearchSinkConfiguration.getConnectionConfiguration()).thenReturn(connectionConfiguration);

        when(pluginMetrics.counter(MetricNames.RECORDS_IN)).thenReturn(mock(Counter.class));
        when(pluginMetrics.timer(MetricNames.TIME_ELAPSED)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(INTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(EXTERNAL_LATENCY)).thenReturn(mock(Timer.class));
        when(pluginMetrics.timer(BULKREQUEST_LATENCY)).thenReturn(mock(Timer.class));
        when(pluginMetrics.counter(BULKREQUEST_ERRORS)).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter(INVALID_ACTION_ERRORS)).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter(DYNAMIC_INDEX_DROPPED_EVENTS)).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter(INVALID_VERSION_EXPRESSION_DROPPED_EVENTS)).thenReturn(mock(Counter.class));
        when(pluginMetrics.summary(BULKREQUEST_SIZE_BYTES)).thenReturn(mock(DistributionSummary.class));

        lenient().when(sinkContext.getTagsTargetKey()).thenReturn(null);
        lenient().when(sinkContext.getIncludeKeys()).thenReturn(null);
        lenient().when(sinkContext.getExcludeKeys()).thenReturn(null);
    }

    private void configureScript(final String source) {
        configureScript(source, null);
    }

    private void configureScript(final String source, final Map<String, Object> params) {
        when(scriptConfiguration.getSource()).thenReturn(source);
        lenient().when(scriptConfiguration.getParams()).thenReturn(params);
        when(indexConfiguration.getScriptConfiguration()).thenReturn(scriptConfiguration);
    }

    private void configureAction(final String action) {
        when(indexConfiguration.getAction()).thenReturn(action);
    }

    private OpenSearchSink createObjectUnderTest() throws IOException {
        try (final MockedStatic<OpenSearchSinkConfiguration> configMockedStatic = mockStatic(OpenSearchSinkConfiguration.class);
             final MockedStatic<PluginMetrics> metricsMockedStatic = mockStatic(PluginMetrics.class);
             final MockedConstruction<IndexManagerFactory> ignored = mockConstruction(IndexManagerFactory.class, (mock, context) -> {
                 indexManagerFactory = mock;
             })) {
            metricsMockedStatic.when(() -> PluginMetrics.fromPluginSetting(pluginSetting)).thenReturn(pluginMetrics);
            configMockedStatic.when(() -> OpenSearchSinkConfiguration.readOSConfig(openSearchSinkConfig, expressionEvaluator))
                    .thenReturn(openSearchSinkConfiguration);
            return new OpenSearchSink(
                    pluginSetting, sinkContext, expressionEvaluator, awsCredentialsSupplier, pipelineDescription, pluginConfigObservable, openSearchSinkConfig);
        }
    }

    private JsonNode serializeBody(final UpdateOperation<?> updateOp) throws IOException {
        final java.util.Iterator<?> parts = updateOp._serializables();
        parts.next(); // skip action line
        final Object body = parts.next();
        final StringWriter writer = new StringWriter();
        try (final JsonGenerator generator = jsonpMapper.jsonProvider().createGenerator(writer)) {
            ((org.opensearch.client.json.PlainJsonSerializable) body).serialize(generator, jsonpMapper);
        }
        return objectMapper.readTree(writer.toString());
    }

    @Test
    void script_sets_script_source_and_lang() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.counter += 1");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"counter\":0}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("script").get("source").asText(), equalTo("ctx._source.counter += 1"));
        assertThat(body.get("script").get("lang").asText(), equalTo("painless"));
    }

    @Test
    void script_always_passes_event_as_params_doc() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.putAll(params.doc)");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("price", 9.99).put("currency", "USD");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        final JsonNode doc = body.get("script").get("params").get("doc");
        assertThat(doc, notNullValue());
        assertThat(doc.get("price").asDouble(), equalTo(9.99));
        assertThat(doc.get("currency").asText(), equalTo("USD"));
    }

    @Test
    void script_merges_resolved_params_alongside_doc() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.counter += params.increment", Map.of("increment", 5));
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.builder()
                .withJsonString("{}")
                .withDocumentId("doc-1")
                .withResolvedScriptParameters(Map.of("increment", 5))
                .build();

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        final JsonNode params = body.get("script").get("params");
        assertThat(params.get("doc"), notNullValue());
        assertThat(params.get("increment").asInt(), equalTo(5));
    }

    @Test
    void script_always_sets_scripted_upsert_true() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.counter += 1");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode();
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("scripted_upsert").asBoolean(), equalTo(true));
    }

    @Test
    void script_sets_upsert_body() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.counter += 1");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"counter\":0}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("upsert"), notNullValue());
    }

    @Test
    void script_works_with_update_action() throws IOException {
        configureAction(OpenSearchBulkActions.UPDATE.toString());
        configureScript("ctx._source.status = params.doc.status");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("status", "active");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("script"), notNullValue());
        assertThat(body.get("scripted_upsert").asBoolean(), equalTo(true));
    }

    @Test
    void script_sets_document_id_and_routing() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        configureScript("ctx._source.counter += 1");
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode();
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "my-doc-id", "my-route", null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        assertThat(result.update().id(), equalTo("my-doc-id"));
        assertThat(result.update().routing(), equalTo("my-route"));
    }

    @Test
    void without_script_upsert_preserves_original_behavior() throws IOException {
        configureAction(OpenSearchBulkActions.UPSERT.toString());
        when(indexConfiguration.getScriptConfiguration()).thenReturn(null);
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.has("script"), equalTo(false));
        assertThat(body.get("doc"), notNullValue());
        assertThat(body.get("upsert"), notNullValue());
    }

    @Test
    void without_script_update_preserves_original_behavior() throws IOException {
        configureAction(OpenSearchBulkActions.UPDATE.toString());
        when(indexConfiguration.getScriptConfiguration()).thenReturn(null);
        final OpenSearchSink sink = createObjectUnderTest();

        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = sink.getBulkOperationForAction(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.has("script"), equalTo(false));
        assertThat(body.get("doc"), notNullValue());
        assertThat(body.has("upsert"), equalTo(false));
    }
}
