/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class OpenSearchSourcePluginMetrics {

    static final String DOCUMENTS_PROCESSED = "documentsProcessed";
    static final String INDICES_PROCESSED = "indicesProcessed";
    static final String INDEX_PROCESSING_TIME_ELAPSED = "indexProcessingTime";
    static final String PROCESSING_ERRORS = "processingErrors";

    private final Counter documentsProcessedCounter;
    private final Counter indicesProcessedCounter;
    private final Counter processingErrorsCounter;
    private final Timer indexProcessingTimeTimer;

    public static OpenSearchSourcePluginMetrics create(final PluginMetrics pluginMetrics) {
        return new OpenSearchSourcePluginMetrics(pluginMetrics);
    }

    private OpenSearchSourcePluginMetrics(final PluginMetrics pluginMetrics) {
        documentsProcessedCounter = pluginMetrics.counter(DOCUMENTS_PROCESSED);
        indicesProcessedCounter = pluginMetrics.counter(INDICES_PROCESSED);
        processingErrorsCounter = pluginMetrics.counter(PROCESSING_ERRORS);
        indexProcessingTimeTimer = pluginMetrics.timer(INDEX_PROCESSING_TIME_ELAPSED);
    }

    public Counter getDocumentsProcessedCounter() {
        return documentsProcessedCounter;
    }

    public Counter getIndicesProcessedCounter() {
        return indicesProcessedCounter;
    }

    public Counter getProcessingErrorsCounter() {
        return processingErrorsCounter;
    }

    public Timer getIndexProcessingTimeTimer() {
        return indexProcessingTimeTimer;
    }
}
