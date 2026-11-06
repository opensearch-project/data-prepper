/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch.indices.AliasDefinition;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.opensearch.indices.ExistsRequest;
import org.opensearch.client.opensearch.indices.GetAliasRequest;
import org.opensearch.client.opensearch.indices.get_alias.IndexAliases;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Detects whether the indices behind an alias are stored with parquet as the primary data format.
 */
public class PluggableDataFormatDetector {
    private static final Logger LOG = LoggerFactory.getLogger(PluggableDataFormatDetector.class);

    static final String INDEX_PLUGGABLE_ENABLED = "index.pluggable.dataformat.enabled";
    static final String INDEX_PLUGGABLE_FORMAT = "index.pluggable.dataformat";
    static final String INDEX_PRIMARY_DATA_FORMAT = "index.composite.primary_data_format";
    static final String CLUSTER_PLUGGABLE_ENABLED = "cluster.pluggable.dataformat.enabled";
    static final String CLUSTER_PLUGGABLE_FORMAT = "cluster.pluggable.dataformat";
    static final String CLUSTER_PRIMARY_DATA_FORMAT = "cluster.composite.primary_data_format";
    static final String CLUSTER_PLUGGABLE_ALLOWLIST = "cluster.pluggable.dataformat.restrict.allowlist";
    static final String PLUGGABLE_FEATURE_FLAG = "opensearch.experimental.feature.pluggable.dataformat.enabled";
    static final String COMPOSITE = "composite";
    static final String PARQUET = "parquet";

    private static final Map<String, String> FLAT_SETTINGS_WITH_DEFAULTS = Map.of(
            "flat_settings", "true", "include_defaults", "true");

    private final OpenSearchClient openSearchClient;

    public PluggableDataFormatDetector(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
    }

    /**
     * Checks the settings of the alias's write index if it exists. Otherwise checks the cluster
     * defaults that the first index created for the alias would get.
     *
     * @param indexAlias The index alias
     * @return Whether the indices use a pluggable data format with parquet as the primary format
     * @throws IOException An exception reading from OpenSearch.
     */
    public boolean isParquetPrimary(final String indexAlias) throws IOException {
        // OpenSearch ignores the pluggable data format settings, on existing and new indices, when this flag is off.
        final JsonNode cluster = getJson("/_cluster/settings", FLAT_SETTINGS_WITH_DEFAULTS);
        if ("false".equals(setting(cluster, PLUGGABLE_FEATURE_FLAG, "transient", "persistent", "defaults"))) {
            return false;
        }

        final Optional<String> writeIndex = findWriteIndex(indexAlias);
        if (writeIndex.isPresent()) {
            final JsonNode index = getJson("/" + writeIndex.get() + "/_settings", FLAT_SETTINGS_WITH_DEFAULTS).path(writeIndex.get());
            return isParquetPrimary(
                    setting(index, INDEX_PLUGGABLE_ENABLED, "settings", "defaults"),
                    setting(index, INDEX_PLUGGABLE_FORMAT, "settings", "defaults"),
                    setting(index, INDEX_PRIMARY_DATA_FORMAT, "settings", "defaults"));
        }

        // Mirrors how index creation applies the cluster defaults to the first index the sink creates.
        if (isAllowlisted(indexAlias + IsmPolicyManagement.DEFAULT_INDEX_SUFFIX, cluster)) {
            return false;
        }
        return isParquetPrimary(
                setting(cluster, CLUSTER_PLUGGABLE_ENABLED, "transient", "persistent", "defaults"),
                setting(cluster, CLUSTER_PLUGGABLE_FORMAT, "transient", "persistent", "defaults"),
                setting(cluster, CLUSTER_PRIMARY_DATA_FORMAT, "transient", "persistent", "defaults"));
    }

    // Exact matches: these values are read back from OpenSearch, which parses them case-sensitively.
    static boolean isParquetPrimary(final String enabled, final String format, final String primaryDataFormat) {
        if (!"true".equals(enabled)) {
            return false;
        }
        return PARQUET.equals(format) || (COMPOSITE.equals(format) && PARQUET.equals(primaryDataFormat));
    }

    private Optional<String> findWriteIndex(final String indexAlias) throws IOException {
        if (openSearchClient.indices().existsAlias(new ExistsAliasRequest.Builder().name(indexAlias).build()).value()) {
            final Map<String, IndexAliases> indices = openSearchClient.indices()
                    .getAlias(new GetAliasRequest.Builder().name(indexAlias).build())
                    .result();
            for (final Map.Entry<String, IndexAliases> index : indices.entrySet()) {
                final AliasDefinition alias = index.getValue().aliases().get(indexAlias);
                if (alias != null && Boolean.TRUE.equals(alias.isWriteIndex())) {
                    return Optional.of(index.getKey());
                }
            }
            // An alias on a single index without is_write_index writes to that index.
            if (indices.size() == 1) {
                return Optional.of(indices.keySet().iterator().next());
            }
            // OpenSearch rejects writes to such an alias, so the sink would fail on any index_type.
            LOG.debug("Alias {} is on {} indices without a write index, checking the cluster defaults instead",
                    indexAlias, indices.size());
            return Optional.empty();
        }
        if (openSearchClient.indices().exists(new ExistsRequest.Builder().index(indexAlias).build()).value()) {
            return Optional.of(indexAlias);
        }
        return Optional.empty();
    }

    private static boolean isAllowlisted(final String indexName, final JsonNode cluster) {
        for (final String section : new String[] {"transient", "persistent", "defaults"}) {
            final JsonNode allowlist = cluster.path(section).path(CLUSTER_PLUGGABLE_ALLOWLIST);
            if (!allowlist.isMissingNode()) {
                for (final JsonNode prefix : allowlist) {
                    if (indexName.startsWith(prefix.asText())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    /**
     * Returns the value of a flat setting from the first section that has it, or null.
     */
    private static String setting(final JsonNode response, final String key, final String... sections) {
        for (final String section : sections) {
            final JsonNode value = response.path(section).path(key);
            if (value.isValueNode()) {
                return value.asText();
            }
        }
        return null;
    }

    private JsonNode getJson(final String path, final Map<String, String> queryParameters) throws IOException {
        final Endpoint<Void, JsonData, ErrorResponse> endpoint = new SimpleEndpoint<>(
                request -> "GET",
                request -> path,
                request -> queryParameters,
                SimpleEndpoint.emptyMap(),
                false,
                JsonData._DESERIALIZER);
        return openSearchClient._transport()
                .performRequest(null, endpoint, openSearchClient._transportOptions())
                .to(JsonNode.class);
    }
}
