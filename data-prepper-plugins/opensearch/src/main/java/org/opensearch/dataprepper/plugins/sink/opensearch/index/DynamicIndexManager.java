/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.RestHighLevelClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DynamicIndexManager implements IndexManager {
    private Cache<String, IndexManager> indexManagerCache;
    final int CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES = 30;
    final int APPROXIMATE_INDEX_MANAGER_SIZE = 32;
    private final long cacheSizeInKB = 1024;
    protected RestHighLevelClient restHighLevelClient;
    protected OpenSearchSinkConfiguration openSearchSinkConfiguration;
    final IndexType indexType;
    private final IndexManagerFactory indexManagerFactory;

    public DynamicIndexManager(final IndexType indexType, final RestHighLevelClient restHighLevelClient, final OpenSearchSinkConfiguration openSearchSinkConfiguration, final IndexManagerFactory indexManagerFactory){
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchSinkConfiguration);
        this.indexType = indexType;
        this.indexManagerFactory = indexManagerFactory;
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchSinkConfiguration = openSearchSinkConfiguration;
        CacheBuilder<String, IndexManager> cacheBuilder = CacheBuilder.newBuilder()
                        .recordStats()
                        .concurrencyLevel(1)
                        .maximumWeight(cacheSizeInKB)
                        .expireAfterAccess(CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES, TimeUnit.MINUTES)
                        .weigher((k, v) -> APPROXIMATE_INDEX_MANAGER_SIZE);
        this.indexManagerCache = cacheBuilder.build();
        checkNotNull(indexManagerCache);
    }

    @Override
    public void setupIndex() throws IOException {
    }

    @Override
    public String getIndexName(final String dynamicIndexAlias) throws IOException {
        String fullIndexAlias = AbstractIndexManager.getIndexAliasWithDate(dynamicIndexAlias);
        IndexManager indexManager = indexManagerCache.getIfPresent(fullIndexAlias);
        if (indexManager == null) {
            indexManager = indexManagerFactory.getIndexManager(indexType, restHighLevelClient, openSearchSinkConfiguration, fullIndexAlias);
            indexManagerCache.put(fullIndexAlias, indexManager);
            indexManager.setupIndex();
        }
        return indexManager.getIndexName(fullIndexAlias);
    }
}

