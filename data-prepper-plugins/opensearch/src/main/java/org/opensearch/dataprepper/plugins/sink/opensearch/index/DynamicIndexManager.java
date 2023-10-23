/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DynamicIndexManager extends AbstractIndexManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicIndexManager.class);
    private static final int INDEX_SETUP_RETRY_WAIT_TIME_MS = 1000;

    private Cache<String, IndexManager> indexManagerCache;
    final int CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES = 30;
    final int APPROXIMATE_INDEX_MANAGER_SIZE = 32;
    private final long cacheSizeInKB = 1024;
    protected RestHighLevelClient restHighLevelClient;
    protected OpenSearchClient openSearchClient;
    protected OpenSearchSinkConfiguration openSearchSinkConfiguration;
    protected ClusterSettingsParser clusterSettingsParser;
    final IndexType indexType;
    private final IndexManagerFactory indexManagerFactory;
    private final TemplateStrategy templateStrategy;

    public DynamicIndexManager(final IndexType indexType,
                               final OpenSearchClient openSearchClient,
                               final RestHighLevelClient restHighLevelClient,
                               final OpenSearchSinkConfiguration openSearchSinkConfiguration,
                               final ClusterSettingsParser clusterSettingsParser,
                               final TemplateStrategy templateStrategy,
                               final IndexManagerFactory indexManagerFactory){
        super(restHighLevelClient, openSearchClient, openSearchSinkConfiguration, clusterSettingsParser, templateStrategy, "");
        this.templateStrategy = templateStrategy;
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchSinkConfiguration);
        checkNotNull(clusterSettingsParser);
        this.indexType = indexType;
        this.indexManagerFactory = indexManagerFactory;
        this.openSearchClient = openSearchClient;
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchSinkConfiguration = openSearchSinkConfiguration;
        this.clusterSettingsParser = clusterSettingsParser;
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
        if (dynamicIndexAlias == null) {
            throw new IOException("index alias is null");
        }
        String fullIndexAlias = AbstractIndexManager.getIndexAliasWithDate(dynamicIndexAlias);
        IndexManager indexManager = indexManagerCache.getIfPresent(fullIndexAlias);
        if (indexManager == null) {
            indexManager = indexManagerFactory.getIndexManager(
                    indexType, openSearchClient, restHighLevelClient, openSearchSinkConfiguration, templateStrategy, fullIndexAlias);
            indexManagerCache.put(fullIndexAlias, indexManager);
            setupIndexWithRetries(indexManager);
        }
        return indexManager.getIndexName(fullIndexAlias);
    }

    private void setupIndexWithRetries(final IndexManager indexManager) throws IOException {
        boolean isIndexSetup = false;

        while (!isIndexSetup) {
            try {
                indexManager.setupIndex();
                isIndexSetup = true;
            } catch (final OpenSearchException e) {
                LOG.warn("Failed to setup dynamic index with an exception. ", e);
                try {
                    Thread.sleep(INDEX_SETUP_RETRY_WAIT_TIME_MS);
                } catch (final InterruptedException ex) {
                    LOG.warn("Interrupted while sleeping between index setup retries");
                }
            }
        }
    }
}

