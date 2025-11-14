/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.concurrent.ConcurrentHashMap;


public class IndexCache {
    private final ConcurrentHashMap<String, Boolean> dataStreamCache;
    
    public IndexCache() {
        this.dataStreamCache = new ConcurrentHashMap<>();
    }

    public void putDataStreamResult(final String indexName, final boolean isDataStream) {
        dataStreamCache.put(indexName, isDataStream);
    }

    public Boolean getDataStreamResult(final String indexName) {
        return dataStreamCache.get(indexName);
    }

    void clearAll() {
        dataStreamCache.clear();
    }
}