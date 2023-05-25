/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * OpenSearch service related implementation
 */
public class OpenSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchService.class);
    private OpenSearchClientBuilder clientBuilder;
    public OpenSearchService(OpenSearchClientBuilder clientBuilder){

        this.clientBuilder = clientBuilder;
    }

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int OPEN_SEARCH_VERSION = 130;

    public List<IndicesRecord> getCatOpenSearchIndices(final OpenSearchClient osClient){

      return null;
    }

    public String getPitId(final String index){
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> searchIndexesByPitId(final String pitId) {
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> searchIndexesByPitIdForPagination(final String pitId) {
       // if response is not 200 then will call BackoffService for retry
        return null;
    }


    public Map<String, Object> scrollIndexesByIndexAndUrl(final String index, final String url) {
        // if response is not 200 then will call BackoffService for retry
        return null;
    }


    public boolean deletePitId(String pitId) {

        return false;
    }

    public boolean deleteScrollId(String scrollId) {

        return false;
    }

    public void processIndexes(final Integer version,
                               final String indexList,
                               final URL url,
                               final Integer batchSize) {
        OpenSearchClient a = clientBuilder.createOpenSearchClient(url);
        Map<String, Object> recordsMap = null;
        if(version > OPEN_SEARCH_VERSION) {
            String pitId = getPitId(indexList);
            if(batchSize > BATCH_SIZE_VALUE)
                recordsMap = searchIndexesByPitIdForPagination(pitId);
            else
                recordsMap = searchIndexesByPitId(pitId);
            deletePitId(pitId);
        } else {
            recordsMap = scrollIndexesByIndexAndUrl(indexList, url.getHost());
            deleteScrollId(recordsMap.get("scroll_id").toString());
        }

        // push recordsMap to Buffer

    }


}
