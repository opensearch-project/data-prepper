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

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.GetDataStreamRequest;
import org.opensearch.client.opensearch.indices.GetDataStreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility class to detect if an index name refers to a Data Stream
 */
public class DataStreamDetector {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamDetector.class);
    
    private final OpenSearchClient openSearchClient;
    private final IndexCache indexCache;
    
    public DataStreamDetector(final OpenSearchClient openSearchClient, final IndexCache indexCache) {
        this.openSearchClient = openSearchClient;
        this.indexCache = indexCache;
    }
    
    /**
     * Determines if the given index name refers to a Data Stream
     * @param indexName the index name to check
     * @return true if it's a Data Stream, false otherwise
     */
    public boolean isDataStream(final String indexName) {
        final Boolean cached = indexCache.getDataStreamResult(indexName);
        if (cached != null) {
            return cached;
        }
        
        final boolean result = checkDataStream(indexName);
        indexCache.putDataStreamResult(indexName, result);
        return result;
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
    

}