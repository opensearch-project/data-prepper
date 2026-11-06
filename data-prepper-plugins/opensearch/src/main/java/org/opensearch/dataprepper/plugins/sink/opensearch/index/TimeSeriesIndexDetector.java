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
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects whether an index is an Amazon OpenSearch Serverless NextGen time series index
 * and which field it uses as its timestamp field.
 */
public class TimeSeriesIndexDetector {
    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesIndexDetector.class);
    static final String TIMESTAMP_FIELD_SETTING = "index.timestamp_field";
    private static final String MISSING_TIMESTAMP_FIELD_PREFIX = "document is missing required timestamp field [";

    private final OpenSearchClient openSearchClient;
    private final IndexCache indexCache;
    private final Set<String> failedLookups = ConcurrentHashMap.newKeySet();

    public TimeSeriesIndexDetector(final OpenSearchClient openSearchClient, final IndexCache indexCache) {
        this.openSearchClient = openSearchClient;
        this.indexCache = indexCache;
    }

    /**
     * @param indexName the index name to check
     * @return the timestamp field of the time series index, or empty if it is not a time series index
     *         or the lookup failed since the last bulk flush
     */
    public Optional<String> getTimestampField(final String indexName) {
        if (indexCache.isTimeSeriesStatusKnown(indexName)) {
            return indexCache.getTimeSeriesTimestampField(indexName);
        }
        if (failedLookups.contains(indexName)) {
            return Optional.empty();
        }

        try {
            final String timestampField = fetchTimestampField(indexName);
            indexCache.putTimeSeriesTimestampField(indexName, timestampField);
            return Optional.ofNullable(timestampField);
        } catch (final Exception e) {
            failedLookups.add(indexName);
            LOG.debug("Time series index detection failed for index '{}', will retry after the next bulk flush: {}",
                    indexName, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Allows indexes whose lookup failed to be looked up again. Called after each bulk flush,
     * since the flush may have created the index.
     */
    public void clearFailedLookups() {
        failedLookups.clear();
    }

    /**
     * @return the timestamp field named by a "document is missing required timestamp field" bulk error, or empty
     */
    public Optional<String> getMissingTimestampField(final BulkResponseItem bulkResponseItem) {
        if (bulkResponseItem.error() == null || bulkResponseItem.error().reason() == null) {
            return Optional.empty();
        }
        final String reason = bulkResponseItem.error().reason();
        if (!reason.startsWith(MISSING_TIMESTAMP_FIELD_PREFIX)) {
            return Optional.empty();
        }
        final int end = reason.indexOf(']', MISSING_TIMESTAMP_FIELD_PREFIX.length());
        return end < 0 ? Optional.empty() : Optional.of(reason.substring(MISSING_TIMESTAMP_FIELD_PREFIX.length(), end));
    }

    public void recordTimestampField(final String indexName, final String timestampField) {
        if (!Optional.of(timestampField).equals(indexCache.getTimeSeriesTimestampField(indexName))) {
            LOG.info("Index '{}' is a time series index with timestamp field '{}'.", indexName, timestampField);
            indexCache.putTimeSeriesTimestampField(indexName, timestampField);
        }
        failedLookups.remove(indexName);
    }

    private String fetchTimestampField(final String indexName) throws IOException {
        final JsonNode indices = openSearchClient._transport().performRequest(
                        null, createGetTimestampFieldEndpoint(indexName), openSearchClient._transportOptions())
                .to(JsonNode.class);
        final Iterator<JsonNode> indexSettings = indices.elements();
        final String timestampField = indexSettings.hasNext()
                ? indexSettings.next().path("settings").path(TIMESTAMP_FIELD_SETTING).textValue()
                : null;
        if (timestampField == null) {
            LOG.debug("Index '{}' has no '{}' setting and is not a time series index.", indexName, TIMESTAMP_FIELD_SETTING);
        }
        return timestampField;
    }

    // The typed GetIndicesSettingsResponse drops index.timestamp_field, which is a Serverless-only setting.
    private static Endpoint<Void, JsonData, ErrorResponse> createGetTimestampFieldEndpoint(final String indexName) {
        final StringBuilder path = new StringBuilder("/");
        SimpleEndpoint.pathEncode(indexName, path);
        path.append("/_settings/").append(TIMESTAMP_FIELD_SETTING);
        final String requestPath = path.toString();

        return new SimpleEndpoint<>(
                request -> "GET",
                request -> requestPath,
                request -> Map.of("flat_settings", "true"),
                SimpleEndpoint.emptyMap(),
                false,
                JsonData._DESERIALIZER);
    }
}
