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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class BulkOperationFactoryTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private BulkOperationFactory factory;

    @BeforeEach
    void setUp() {
        final ScriptManager scriptManager = new ScriptManager(null, null);
        factory = new BulkOperationFactory(VersionType.External, scriptManager, objectMapper, false);
    }

    @Test
    void create_action_returns_create_operation() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"key\":\"val\"}", "doc-1", "route-1", null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.CREATE.toString(), document, null, "test-index", objectMapper.createObjectNode());

        assertThat(result.isCreate(), is(true));
        assertThat(result.create().index(), equalTo("test-index"));
        assertThat(result.create().id(), equalTo("doc-1"));
        assertThat(result.create().routing(), equalTo("route-1"));
    }

    @Test
    void index_action_returns_index_operation() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"key\":\"val\"}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.INDEX.toString(), document, 1L, "test-index", objectMapper.createObjectNode());

        assertThat(result.isIndex(), is(true));
        assertThat(result.index().index(), equalTo("test-index"));
        assertThat(result.index().id(), equalTo("doc-1"));
        assertThat(result.index().version(), equalTo(1L));
        assertThat(result.index().versionType(), equalTo(VersionType.External));
    }

    @Test
    void delete_action_returns_delete_operation() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", "route-1", null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.DELETE.toString(), document, 2L, "test-index", objectMapper.createObjectNode());

        assertThat(result.isDelete(), is(true));
        assertThat(result.delete().index(), equalTo("test-index"));
        assertThat(result.delete().id(), equalTo("doc-1"));
        assertThat(result.delete().routing(), equalTo("route-1"));
        assertThat(result.delete().version(), equalTo(2L));
    }

    @Test
    void update_action_returns_update_operation() {
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        assertThat(result.isUpdate(), is(true));
        assertThat(result.update().index(), equalTo("test-index"));
        assertThat(result.update().id(), equalTo("doc-1"));
    }

    @Test
    void upsert_action_returns_update_operation() {
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "test");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"test\"}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.UPSERT.toString(), document, null, "test-index", jsonNode);

        assertThat(result.isUpdate(), is(true));
        assertThat(result.update().index(), equalTo("test-index"));
        assertThat(result.update().id(), equalTo("doc-1"));
    }

    @Test
    void unknown_action_defaults_to_index_operation() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, null);

        final BulkOperation result = factory.create(
                "unknown_action", document, null, "test-index", objectMapper.createObjectNode());

        assertThat(result.isIndex(), is(true));
        assertThat(result.index().index(), equalTo("test-index"));
    }

    @Test
    void create_action_without_optional_fields() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", null, null, null);

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.CREATE.toString(), document, null, "test-index", objectMapper.createObjectNode());

        assertThat(result.isCreate(), is(true));
        assertThat(result.create().index(), equalTo("test-index"));
    }

    @Test
    void update_with_document_filters_deserializes_from_serialized_json() {
        final ScriptManager scriptManager = new ScriptManager(null, null);
        final BulkOperationFactory filterFactory = new BulkOperationFactory(null, scriptManager, objectMapper, true);
        final ObjectNode jsonNode = objectMapper.createObjectNode().put("name", "original");
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{\"name\":\"filtered\"}", "doc-1", null, null);

        final BulkOperation result = filterFactory.create(
                OpenSearchBulkActions.UPDATE.toString(), document, null, "test-index", jsonNode);

        assertThat(result.isUpdate(), is(true));
        assertThat(result.update().id(), equalTo("doc-1"));
    }

    @Test
    void create_action_sets_pipeline() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, "my-pipeline");

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.CREATE.toString(), document, null, "test-index", objectMapper.createObjectNode());

        assertThat(result.isCreate(), is(true));
        assertThat(result.create().pipeline(), equalTo("my-pipeline"));
    }

    @Test
    void index_action_sets_pipeline() {
        final SerializedJson document = SerializedJson.fromStringAndOptionals("{}", "doc-1", null, "my-pipeline");

        final BulkOperation result = factory.create(
                OpenSearchBulkActions.INDEX.toString(), document, null, "test-index", objectMapper.createObjectNode());

        assertThat(result.isIndex(), is(true));
        assertThat(result.index().pipeline(), equalTo("my-pipeline"));
    }
}
