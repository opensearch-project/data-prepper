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
    private final String documentIdField;
    private final String documentId;
    private final String routingField;
    private final String routing;
    
    public DataStreamIndex(final DataStreamDetector dataStreamDetector, final String documentIdField, final String documentId, final String routingField, final String routing) {
        this.dataStreamDetector = dataStreamDetector;
        this.documentIdField = documentIdField;
        this.documentId = documentId;
        this.routingField = routingField;
        this.routing = routing;
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
        if (dataStreamDetector.isDataStream(indexName) && !event.containsKey(TIMESTAMP_FIELD)) {
            event.put(TIMESTAMP_FIELD, event.getEventHandle().getInternalOriginationTime());
        }
    }

    private void validateConfigurationForDataStream(final String indexName) {
        if (documentIdField != null || documentId != null) {
            LOG.warn("Data Stream '{}' with document ID configuration uses first-write-wins behavior. Subsequent writes to the same ID will be ignored.", indexName);
        }
        if (routingField != null || routing != null) {
            LOG.warn("Data Stream '{}' does not support routing. Routing configuration will be ignored.", indexName);
        }
    }
    
    public void validateDataStreamCompatibility(final String indexName, final String documentId, final String routing) {
        if (dataStreamDetector.isDataStream(indexName)) {
            if (documentId != null) {
                LOG.warn("Data Stream '{}' with document ID '{}' uses first-write-wins behavior. Subsequent writes to the same ID will be ignored.", indexName, documentId);
            }
            if (routing != null) {
                LOG.warn("Data Stream '{}' does not support routing. Routing '{}' will be ignored.", indexName, routing);
            }
        }
    }
}