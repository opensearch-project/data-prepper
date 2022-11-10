/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.google.common.collect.ImmutableSet;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexTemplatesRequest;
import org.opensearch.client.indices.GetIndexTemplatesResponse;
import org.opensearch.client.indices.IndexTemplateMetadata;
import org.opensearch.client.indices.IndexTemplatesExistRequest;
import org.opensearch.client.indices.PutIndexTemplateRequest;
import org.opensearch.dataprepper.plugins.sink.opensearch.OpenSearchSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opensearch.dataprepper.plugins.sink.opensearch.index.IndexManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

public class DynamicIndexManager implements IndexManager {
    private Cache<String, IndexManager> indexManagerCache;
    final int CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES = 30;
    private final long cacheSizeInKB = 1024;
    protected RestHighLevelClient restHighLevelClient;
    protected OpenSearchSinkConfiguration openSearchSinkConfiguration;
    final IndexType indexType;
    private final IndexManagerFactory indexManagerFactory;

    public DynamicIndexManager(final IndexType indexType, final RestHighLevelClient restHighLevelClient, final OpenSearchSinkConfiguration openSearchSinkConfiguration){
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchSinkConfiguration);
	this.indexType = indexType;
        this.indexManagerFactory = new IndexManagerFactory();
        this.restHighLevelClient = restHighLevelClient;
        this.openSearchSinkConfiguration = openSearchSinkConfiguration;
        CacheBuilder<String, IndexManager> cacheBuilder = CacheBuilder.newBuilder()
	                .recordStats()
		        .concurrencyLevel(1)
		        .maximumWeight(cacheSizeInKB)
			.expireAfterAccess(CACHE_EXPIRE_AFTER_ACCESS_TIME_MINUTES, TimeUnit.MINUTES)
	                .weigher((k, v) -> k.toString().length());
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
	}
	indexManager.setupIndex();
	return indexManager.getIndexName(null);
    }
    
    @Override
    public String getIndexName() {
        return null;
    }
}

