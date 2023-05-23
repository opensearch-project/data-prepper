/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;

import java.util.List;
import java.util.Map;

/**
 * OpenSearch service related implementation
 */
public class OpenSearchService {

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int OPEN_SEARCH_VERSION = 130;

    public List<IndicesRecord> getCatOpenSearchIndices(final OpenSearchClient osClient){

      return null;
    }

    public String getPITId(final String index){
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> searchIndexesByPITId(final String pitId) {
        // if response is not 200 then will call BackoffService for retry
        return null;
    }

    public Map<String, Object> searchIndexesByPITIdForPagination(final String pitId) {
       // if response is not 200 then will call BackoffService for retry
        return null;
    }


    public Map<String, Object> scrollIndexesByIndexAndUrl(final String index, final String url) {
        // if response is not 200 then will call BackoffService for retry
        return null;
    }


    public boolean deletePITId(String pitId) {

        return false;
    }

    public boolean deleteScrollId(String scrollId) {

        return false;
    }

    public void processIndexes(final Integer version,
                               final String indexList,
                               final String url,
                               final Integer batchSize) {
        Map<String, Object> recordsMap = null;
        if(version > OPEN_SEARCH_VERSION) {
            String pitId = getPITId(indexList);
            if(batchSize > BATCH_SIZE_VALUE)
                recordsMap = searchIndexesByPITIdForPagination(pitId);
            else
                recordsMap = searchIndexesByPITId(pitId);
            deletePITId(pitId);
        } else {
            recordsMap = scrollIndexesByIndexAndUrl(indexList, url);
            deleteScrollId(recordsMap.get("scroll_id").toString());
        }

        // push recordsMap to Buffer

    }
}
