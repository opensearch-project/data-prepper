/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class DataStreamIndex {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamIndex.class);
    private static final String TIMESTAMP_FIELD = "@timestamp";
    
    private final DataStreamDetector dataStreamDetector;
    private final IndexConfiguration indexConfiguration;

    // These notices describe configured, expected behavior for a data stream rather than a
    // per-record fault, so they are logged at most once per data stream instead of once per
    // ingested document to avoid overwhelming the logs on high-volume pipelines.
    private final Set<String> loggedNotices = ConcurrentHashMap.newKeySet();

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
                !configuredAction.equals(OpenSearchBulkActions.INDEX.toString()) &&
                logNoticeOnce(indexName, "action:" + configuredAction)) {
                LOG.warn("Data Stream '{}' requires 'create' action, but '{}' was configured. Using 'create' action.", 
                        indexName, configuredAction);
            }
            return OpenSearchBulkActions.CREATE.toString();
        }
        return configuredAction != null ? configuredAction : OpenSearchBulkActions.INDEX.toString();
    }
    

    public void ensureTimestamp(final Event event, final String indexName) {
        if (dataStreamDetector.isDataStream(indexName) && !event.containsKey(TIMESTAMP_FIELD)) {
            event.put(TIMESTAMP_FIELD, event.getEventHandle().getInternalOriginationTime().toEpochMilli());
        }
    }

    private void validateConfigurationForDataStream(final String indexName) {
        if ((indexConfiguration.getDocumentIdField() != null || indexConfiguration.getDocumentId() != null)
                && logNoticeOnce(indexName, "documentId")) {
            LOG.warn("Data Stream '{}' with document ID configuration uses first-write-wins behavior. Subsequent writes to the same ID will be ignored.", indexName);
        }
        if ((indexConfiguration.getRoutingField() != null || indexConfiguration.getRouting() != null)
                && logNoticeOnce(indexName, "routing")) {
            LOG.warn("Data Stream '{}' does not support routing. Routing configuration will be ignored.", indexName);
        }
    }

    /**
     * Returns {@code true} only the first time a given notice is seen for a data stream, so that
     * configuration notices are logged once per data stream rather than once per ingested document.
     * Package-private for testing.
     */
    boolean logNoticeOnce(final String indexName, final String noticeType) {
        return loggedNotices.add(indexName + " " + noticeType);
    }
    

}