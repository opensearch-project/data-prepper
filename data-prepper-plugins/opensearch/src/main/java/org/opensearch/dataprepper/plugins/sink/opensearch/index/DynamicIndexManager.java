/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class DynamicIndexManager extends AbstractIndexManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicIndexManager.class);
    private static final int INDEX_SETUP_RETRY_WAIT_TIME_MS = 1000;

    private Cache<String, IndexManager> indexManagerCache;
    final int CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES = 30;
    final int APPROXIMATE_INDEX_MANAGER_SIZE = 32;
    private static final String INVALID_INDEX_CHARACTERS = "[:\\\"*+/\\\\|?#><]";
    private static final Pattern INVALID_REGEX_CHARACTERS_PATTERN = Pattern.compile(INVALID_INDEX_CHARACTERS);
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
        Caffeine<String, IndexManager> cacheBuilder = Caffeine.newBuilder()
                        .recordStats()
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

        if (openSearchSinkConfiguration.getIndexConfiguration().isNormalizeIndex()) {
            fullIndexAlias = normalizeIndex(fullIndexAlias);
        }

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
    // Restrictions on index names (https://opensearch.org/docs/1.0/opensearch/rest-api/create-index/#index-naming-restrictions)
    private String normalizeIndex(final String indexName) {
        String normalizedIndexName = indexName.toLowerCase(Locale.ROOT);

        normalizedIndexName = INVALID_REGEX_CHARACTERS_PATTERN.matcher(normalizedIndexName).replaceAll("");

        while (normalizedIndexName.startsWith("_") || normalizedIndexName.startsWith("-")) {
            if (normalizedIndexName.length() == 1) {
                throw new RuntimeException(String.format(
                        "Unable to normalize index '%s'. This index name is invalid.", indexName)
                );
            }

            normalizedIndexName = normalizedIndexName.substring(1);
        }

        if (normalizedIndexName.isBlank()) {
            throw new RuntimeException(String.format(
                    "Unable to normalize index '%s'. The result after normalization was an empty String.", indexName)
            );
        }

        return normalizedIndexName;
    }
}

