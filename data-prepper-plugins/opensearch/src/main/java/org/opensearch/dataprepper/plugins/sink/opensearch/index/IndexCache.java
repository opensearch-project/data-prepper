/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class IndexCache {
    private final ConcurrentHashMap<String, Boolean> dataStreamCache;
    private final ConcurrentHashMap<String, String> timeSeriesTimestampFieldCache;
    private final Set<String> nonTimeSeriesIndexes;
    
    public IndexCache() {
        this.dataStreamCache = new ConcurrentHashMap<>();
        this.timeSeriesTimestampFieldCache = new ConcurrentHashMap<>();
        this.nonTimeSeriesIndexes = ConcurrentHashMap.newKeySet();
    }

    /**
     * @param indexName the index name
     * @param timestampField the timestamp field of the time series index, or null if it is not a time series index
     */
    public void putTimeSeriesTimestampField(final String indexName, final String timestampField) {
        if (timestampField == null) {
            timeSeriesTimestampFieldCache.remove(indexName);
            nonTimeSeriesIndexes.add(indexName);
        } else {
            timeSeriesTimestampFieldCache.put(indexName, timestampField);
            nonTimeSeriesIndexes.remove(indexName);
        }
    }

    public Optional<String> getTimeSeriesTimestampField(final String indexName) {
        return Optional.ofNullable(timeSeriesTimestampFieldCache.get(indexName));
    }

    /**
     * @return true if the index is known to be a time series index or known not to be one
     */
    public boolean isTimeSeriesStatusKnown(final String indexName) {
        return timeSeriesTimestampFieldCache.containsKey(indexName) || nonTimeSeriesIndexes.contains(indexName);
    }

    public void putDataStreamResult(final String indexName, final boolean isDataStream) {
        dataStreamCache.put(indexName, isDataStream);
    }

    public Boolean getDataStreamResult(final String indexName) {
        return dataStreamCache.get(indexName);
    }

    public void clearAll() {
        dataStreamCache.clear();
        timeSeriesTimestampFieldCache.clear();
        nonTimeSeriesIndexes.clear();
    }
}