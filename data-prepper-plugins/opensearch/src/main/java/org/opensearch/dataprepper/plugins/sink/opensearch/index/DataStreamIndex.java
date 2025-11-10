/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataStreamIndex {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamIndex.class);
    private static final String TIMESTAMP_FIELD = "@timestamp";
    
    private final DataStreamDetector dataStreamDetector;
    private final IndexConfiguration indexConfiguration;
    
    public DataStreamIndex(final DataStreamDetector dataStreamDetector, final IndexConfiguration indexConfiguration) {
        this.dataStreamDetector = dataStreamDetector;
        this.indexConfiguration = indexConfiguration;
    }
    

    public String determineAction(final String configuredAction, final String indexName) {
        if (dataStreamDetector.isDataStream(indexName)) {
            validateConfigurationForDataStream(indexName);
            
            // Only warn if user explicitly configured a non-create action (excluding the default "index" action)
            if (configuredAction != null && 
                !configuredAction.equals(OpenSearchBulkActions.CREATE.toString()) &&
                !configuredAction.equals(OpenSearchBulkActions.INDEX.toString())) {
                LOG.warn("Data Stream '{}' requires 'create' action, but '{}' was configured. Using 'create' action.", 
                        indexName, configuredAction);
            }
            return OpenSearchBulkActions.CREATE.toString();
        }
        return configuredAction != null ? configuredAction : OpenSearchBulkActions.INDEX.toString();
    }
    

    public void ensureTimestamp(final Event event, final String indexName) {
        LOG.info("ensureTimestamp called for index: {}", indexName);
        final boolean isDataStream = dataStreamDetector.isDataStream(indexName);
        final boolean hasTimestamp = event.containsKey(TIMESTAMP_FIELD);
        LOG.info("Index '{}' - isDataStream: {}, hasTimestamp: {}", indexName, isDataStream, hasTimestamp);
        
        if (isDataStream && !hasTimestamp) {
            event.put(TIMESTAMP_FIELD, event.getEventHandle().getInternalOriginationTime().toEpochMilli());
            LOG.info("Added @timestamp to event for data stream '{}'", indexName);
        }
    }

    private void validateConfigurationForDataStream(final String indexName) {
        if (indexConfiguration.getDocumentIdField() != null || indexConfiguration.getDocumentId() != null) {
            LOG.warn("Data Stream '{}' with document ID configuration uses first-write-wins behavior. Subsequent writes to the same ID will be ignored.", indexName);
        }
        if (indexConfiguration.getRoutingField() != null || indexConfiguration.getRouting() != null) {
            LOG.warn("Data Stream '{}' does not support routing. Routing configuration will be ignored.", indexName);
        }
    }
    

}