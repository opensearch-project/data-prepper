/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;

import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.json.simple.JSONObject;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Reference to ElasticSearch Api calls for Higher and Lower Versions
 */

public class ElasticSearchApiCalls implements SearchAPICalls {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApiCalls.class);

    private static final String KEEP_ALIVE_VALUE = "24h";

    private static final String TIME_VALUE = "24h";

    private static final int ELASTIC_SEARCH_VERSION = 7100;

    private static final int SEARCH_AFTER_SIZE = 500;

    private ElasticsearchClient elasticsearchClient;

    private SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();

    public ElasticSearchApiCalls(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) {
        OpenPointInTimeResponse response = null;
        OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().
                index(openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude()).
                keepAlive(new Time.Builder().time(KEEP_ALIVE_VALUE).build()).build();
            try {
                response = elasticsearchClient.openPointInTime(request);
            } catch (Exception ex){
                LOG.error(" {}",ex.getMessage());
            }
    }

    @Override
    public void searchPitIndexes(final String pitID , final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) {
        SearchResponse<ObjectNode> searchResponse = null;
        try {
            searchResponse = elasticsearchClient.search(req ->
                            req.index(openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude()),
                    ObjectNode.class);
            searchResponse.hits().hits().stream()
                    .map(Hit::source).collect(Collectors.toList());
            sourceInfoProvider.writeClusterDataToBuffer(searchResponse.fields().toString(),buffer);
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void getScrollResponse(final OpenSearchSourceConfiguration openSearchSourceConfiguration,Buffer<Record<Event>> buffer) {
      LOG.info("Available in next PR");
    }

    private SearchResponse getSearchForSort(final OpenSearchSourceConfiguration openSearchSourceConfiguration, long searchAfter, List<SortOptions> sortOptionsList ) {

        SearchResponse response = null;
        SearchRequest searchRequest = null;

        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        if (openSearchSourceConfiguration.getQueryParameterConfiguration().getFields() != null) {
            String[] queryParam = openSearchSourceConfiguration.getQueryParameterConfiguration().getFields().get(0).split(":");
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).query(q -> q.match(t -> t
                                    .field(queryParam[0].trim())
                                    .query(queryParam[1].trim()))).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                                    .sort(sortOptionsList));
        } else {
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(sortOptionsList));
        }
        try {
            response = elasticsearchClient.search(searchRequest, JSONObject.class);
        }catch(IOException e) {
            LOG.error("Exception occured in getSearchForSort {} ", e.getMessage());
        }
        return response;
    }

    public void searchPitIndexesForPagination(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final ElasticsearchClient client, long currentSearchAfterValue, Buffer<Record<Event>> buffer, int currentBatchSize) throws TimeoutException {

        List<SortOptions> sortOptionsList = getSortOption(openSearchSourceConfiguration);
        SearchResponse response = getSearchForSort(openSearchSourceConfiguration,currentSearchAfterValue, sortOptionsList);
        currentBatchSize = currentBatchSize - SEARCH_AFTER_SIZE;
        currentSearchAfterValue = extractSortValue(response, buffer);
        if(currentBatchSize > 0) {
            searchPitIndexesForPagination(openSearchSourceConfiguration, client, currentSearchAfterValue,buffer, currentBatchSize);
        }
    }

    private List<SortOptions> getSortOption(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        List<SortOptions> sortOptionsList = new ArrayList<>();
        for(int sortIndex = 0 ; sortIndex < openSearchSourceConfiguration.getSearchConfiguration().getSorting().size() ; sortIndex++) {

            String sortOrder = openSearchSourceConfiguration.getSearchConfiguration().getSorting().get(sortIndex).getOrder();
            SortOrder order = sortOrder.toLowerCase().equalsIgnoreCase("asc") ? SortOrder.Asc : SortOrder.Desc;
            int finalSortIndex = sortIndex;
            SortOptions sortOptions = new SortOptions.Builder().field(f -> f.field(openSearchSourceConfiguration.getSearchConfiguration()
                    .getSorting().get(finalSortIndex).getSortKey()).order(order)).build();
            sortOptionsList.add(sortOptions);
        }
        return sortOptionsList;
    }

    private long extractSortValue(SearchResponse response, Buffer<Record<Event>> buffer) throws TimeoutException {
        HitsMetadata hitsMetadata = response.hits();
        int size = hitsMetadata.hits().size();
        long sortValue = 0;
        if(size != 0) {
            try {
                sortValue = ((Hit<Object>) hitsMetadata.hits().get(size - 1)).sort().get(0).longValue();
            }catch(Exception e){
                LOG.error("Exception occured in extractSortValue {} ", e.getMessage());
            }
        }
        sourceInfoProvider.writeClusterDataToBuffer(response.fields().toString(),buffer);
        return sortValue;
    }
}
