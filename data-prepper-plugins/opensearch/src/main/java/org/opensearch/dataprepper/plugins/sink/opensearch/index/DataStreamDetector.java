/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.GetDataStreamRequest;
import org.opensearch.client.opensearch.indices.GetDataStreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class to detect if an index name refers to a Data Stream
 */
public class DataStreamDetector {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamDetector.class);
    
    private final OpenSearchClient openSearchClient;
    private final ConcurrentHashMap<String, Boolean> dataStreamCache;
    
    public DataStreamDetector(final OpenSearchClient openSearchClient) {
        this.openSearchClient = openSearchClient;
        this.dataStreamCache = new ConcurrentHashMap<>();
    }
    
    /**
     * Determines if the given index name refers to a Data Stream
     * @param indexName the index name to check
     * @return true if it's a Data Stream, false otherwise
     */
    public boolean isDataStream(final String indexName) {
        return dataStreamCache.computeIfAbsent(indexName, this::checkDataStream);
    }
    
    private boolean checkDataStream(final String indexName) {
        try {
            final GetDataStreamRequest request = GetDataStreamRequest.of(r -> r.name(indexName));
            final GetDataStreamResponse response = openSearchClient.indices().getDataStream(request);
            
            // If we get a response without exception, it's a data stream
            return response.dataStreams() != null && !response.dataStreams().isEmpty();
                    
        } catch (final IOException e) {
            // If we get a 404 or similar, it's not a data stream
            LOG.debug("Index '{}' is not a Data Stream: {}", indexName, e.getMessage());
            return false;
        } catch (final Exception e) {
            LOG.debug("Data Stream detection not supported or failed for index '{}': {}", indexName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Clears the cache for testing purposes
     */
    void clearCache() {
        dataStreamCache.clear();
    }
}