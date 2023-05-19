/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

/**
 * Reference to Schedular for ElasticSearch with Pagination
 */
public class ElasticSearchPITPaginationTask extends TimerTask {
    
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchPITPaginationTask.class);
    
    private OpenSearchSourceConfiguration openSearchSourceConfiguration = null;

    private ElasticsearchClient esClient = null;

    private Buffer<Record<Event>> buffer = null;

    private ElasticSearchApiCalls elasticSearchApiCalls = null;

    private int currentBatchSize = 0;

    public ElasticSearchPITPaginationTask(OpenSearchSourceConfiguration openSearchSourceConfiguration , Buffer<Record<Event>> buffer , ElasticsearchClient esClient ) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        elasticSearchApiCalls = new ElasticSearchApiCalls(esClient);
    }

    @Override
    public void run() {
        int numRuns = 1;
        while (numRuns++ <= openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount()) {
            try {
                currentBatchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();
                elasticSearchApiCalls.searchPitIndexesForPagination(openSearchSourceConfiguration, esClient, 0L, buffer, currentBatchSize);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
