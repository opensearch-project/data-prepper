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
import jakarta.json.stream.JsonGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;
import org.opensearch.dataprepper.plugins.sink.opensearch.configuration.ScriptConfiguration;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

public class OpenSearchSinkScriptTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();

    private BulkOperationFactory createFactory(final ScriptConfiguration scriptConfig) {
        final ScriptManager scriptManager = new ScriptManager(scriptConfig, null);
        return new BulkOperationFactory(null, scriptManager, new ObjectMapper(), false);
    }

    private ScriptConfiguration mockScript(final String source, final Map<String, Object> params) {
        final ScriptConfiguration config = mock(ScriptConfiguration.class);
        lenient().when(config.getSource()).thenReturn(source);
        lenient().when(config.getParams()).thenReturn(params);
        return config;
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
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.counter += 1", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"counter\":0}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("script").get("source").asText(), equalTo("ctx._source.counter += 1"));
        assertThat(body.get("script").get("lang").asText(), equalTo("painless"));
    }

    @Test
    void script_always_passes_event_as_params_doc() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.putAll(params.doc)", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("price", 9.99).put("currency", "USD");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        final JsonNode doc = body.get("script").get("params").get("doc");
        assertThat(doc, notNullValue());
        assertThat(doc.get("price").asDouble(), equalTo(9.99));
        assertThat(doc.get("currency").asText(), equalTo("USD"));
    }

    @Test
    void script_merges_resolved_params_alongside_doc() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.counter += params.increment", Map.of("increment", 5)));
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.builder()
                .withJsonString("{}")
                .withDocumentId("doc-1")
                .withResolvedScriptParameters(Map.of("increment", 5))
                .build();

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        final JsonNode params = body.get("script").get("params");
        assertThat(params.get("doc"), notNullValue());
        assertThat(params.get("increment").asInt(), equalTo(5));
    }

    @Test
    void script_always_sets_scripted_upsert_true() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.counter += 1", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode();
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("scripted_upsert").asBoolean(), equalTo(true));
    }

    @Test
    void script_sets_upsert_body() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.counter += 1", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("counter", 0);
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"counter\":0}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("upsert"), notNullValue());
    }

    @Test
    void script_works_with_update_action() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.status = params.doc.status", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("status", "active");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.get("script"), notNullValue());
        assertThat(body.has("scripted_upsert"), equalTo(false));
        assertThat(body.has("upsert"), equalTo(false));
    }

    @Test
    void script_sets_document_id_and_routing() throws IOException {
        final BulkOperationFactory factory = createFactory(mockScript("ctx._source.counter += 1", null));
        final ObjectNode jsonNode = objectMapper.createObjectNode();
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "my-doc-id", "my-route", null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        assertThat(result.update().id(), equalTo("my-doc-id"));
        assertThat(result.update().routing(), equalTo("my-route"));
    }

    @Test
    void without_script_upsert_preserves_original_behavior() throws IOException {
        final BulkOperationFactory factory = createFactory(null);
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.has("script"), equalTo(false));
        assertThat(body.get("doc"), notNullValue());
        assertThat(body.get("upsert"), notNullValue());
    }

    @Test
    void without_script_update_preserves_original_behavior() throws IOException {
        final BulkOperationFactory factory = createFactory(null);
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        final JsonNode body = serializeBody(result.update());
        assertThat(body.has("script"), equalTo(false));
        assertThat(body.get("doc"), notNullValue());
        assertThat(body.has("upsert"), equalTo(false));
    }
}
