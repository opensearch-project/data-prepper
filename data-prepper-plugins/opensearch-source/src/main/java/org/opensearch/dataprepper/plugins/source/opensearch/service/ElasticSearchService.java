/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchClientBuilder;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch service related implementation
 */
public class ElasticSearchService {

    private OpenSearchClientBuilder clientBuilder;
    public ElasticSearchService(OpenSearchClientBuilder clientBuilder){
        this.clientBuilder = clientBuilder;
    }
    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int ELASTIC_SEARCH_VERSION = 7100;

    public List<IndicesRecord> getCatElasticSearchIndices(final ElasticsearchClient esClient){
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> searchIndexes() {
    // without pagination logic need to be handled here
    // if response is not 200 then will call BackoffService for retry
        return null;
    }
    public Map<String, Object> searchIndexesForPagination() {
        // pagination logic need to be handled here
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> scrollIndexesByIndexAndUrl(final String index, final String url) {
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public boolean deleteScrollId(String scrollId) {

        return false;
    }

    public void processIndexes(final Integer version,
                               final String indexList,
                               final URL url,
                               final Integer batchSize) {
        clientBuilder.createElasticSearchClient(url);
        Map<String, Object> recordsMap = null;
        if(version > ELASTIC_SEARCH_VERSION) {
            if(batchSize > BATCH_SIZE_VALUE) {
                recordsMap = searchIndexesForPagination();
            } else
                recordsMap = searchIndexes();
        } else{
            recordsMap =scrollIndexesByIndexAndUrl(indexList, url.getHost());
            deleteScrollId(recordsMap.get("scroll_id").toString());
        }
        // push recordsMap to Buffer
    }
}
