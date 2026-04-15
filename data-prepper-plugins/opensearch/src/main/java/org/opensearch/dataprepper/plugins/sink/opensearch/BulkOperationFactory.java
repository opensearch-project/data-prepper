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
import org.apache.commons.lang3.StringUtils;
import org.opensearch.client.opensearch._types.VersionType;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.CreateOperation;
import org.opensearch.client.opensearch.core.bulk.DeleteOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.bulk.UpdateOperation;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.sink.opensearch.bulk.SerializedJson;

import java.io.IOException;
import java.util.Optional;

public class BulkOperationFactory {

    private final VersionType versionType;
    private final ScriptManager scriptManager;
    private final ObjectMapper objectMapper;
    private final boolean usingDocumentFilters;

    public BulkOperationFactory(final VersionType versionType,
                                final ScriptManager scriptManager,
                                final ObjectMapper objectMapper,
                                final boolean usingDocumentFilters) {
        this.versionType = versionType;
        this.scriptManager = scriptManager;
        this.objectMapper = objectMapper;
        this.usingDocumentFilters = usingDocumentFilters;
    }

    public BulkOperation create(final String action,
                                final SerializedJson document,
                                final Long version,
                                final String indexName,
                                final JsonNode jsonNode) {
        final Optional<String> docId = document.getDocumentId();
        final Optional<String> routing = document.getRoutingField();
        final Optional<String> pipeline = document.getPipelineField();

        if (StringUtils.equals(action, OpenSearchBulkActions.CREATE.toString())) {
            final CreateOperation.Builder<Object> builder =
                    new CreateOperation.Builder<>()
                            .index(indexName)
                            .document(document);
            docId.ifPresent(builder::id);
            routing.ifPresent(builder::routing);
            pipeline.ifPresent(builder::pipeline);
            return new BulkOperation.Builder().create(builder.build()).build();
        }

        if (StringUtils.equals(action, OpenSearchBulkActions.UPDATE.toString()) ||
                StringUtils.equals(action, OpenSearchBulkActions.UPSERT.toString())) {
            return createUpdateOperation(action, document, version, indexName, jsonNode, docId, routing);
        }

        if (StringUtils.equals(action, OpenSearchBulkActions.DELETE.toString())) {
            final DeleteOperation.Builder builder = new DeleteOperation.Builder()
                    .index(indexName)
                    .versionType(versionType)
                    .version(version);
            docId.ifPresent(builder::id);
            routing.ifPresent(builder::routing);
            return new BulkOperation.Builder().delete(builder.build()).build();
        }

        // Default to "index"
        final IndexOperation.Builder<Object> builder = new IndexOperation.Builder<>()
                .index(indexName)
                .document(document)
                .version(version)
                .versionType(versionType);
        docId.ifPresent(builder::id);
        routing.ifPresent(builder::routing);
        pipeline.ifPresent(builder::pipeline);
        return new BulkOperation.Builder().index(builder.build()).build();
    }

    private BulkOperation createUpdateOperation(final String action,
                                                final SerializedJson document,
                                                final Long version,
                                                final String indexName,
                                                final JsonNode jsonNode,
                                                final Optional<String> docId,
                                                final Optional<String> routing) {
        JsonNode filteredJsonNode = jsonNode;
        try {
            if (usingDocumentFilters) {
                filteredJsonNode = objectMapper.reader().readTree(document.getSerializedJson());
            }
        } catch (final IOException e) {
            throw new RuntimeException(
                    String.format("An exception occurred while deserializing a document for the %s action: %s", action, e.getMessage()));
        }

        final boolean isUpsert = StringUtils.equals(action.toLowerCase(), OpenSearchBulkActions.UPSERT.toString());
        final UpdateOperation.Builder<Object> builder = new UpdateOperation.Builder<>()
                .index(indexName)
                .versionType(versionType)
                .version(version);

        if (scriptManager.isScriptEnabled()) {
            builder.script(scriptManager.buildScript(filteredJsonNode, document.getResolvedScriptParameters().orElse(null)));
            if (isUpsert) {
                builder.upsert(filteredJsonNode);
                builder.scriptedUpsert(true);
                builder.retryOnConflict(3);
            }
        } else if (isUpsert) {
            builder.document(filteredJsonNode).upsert(filteredJsonNode);
        } else {
            builder.document(filteredJsonNode);
        }

        docId.ifPresent(builder::id);
        routing.ifPresent(builder::routing);
        return new BulkOperation.Builder().update(builder.build()).build();
    }
}
