/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class OpenSearchSourcePluginMetrics {

    static final String DOCUMENTS_PROCESSED = "documentsProcessed";
    static final String INDICES_PROCESSED = "indicesProcessed";
    static final String INDEX_PROCESSING_TIME_ELAPSED = "indexProcessingTime";
    static final String PROCESSING_ERRORS = "processingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    private final Counter documentsProcessedCounter;
    private final Counter indicesProcessedCounter;
    private final Counter processingErrorsCounter;
    private final Timer indexProcessingTimeTimer;

    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    public static OpenSearchSourcePluginMetrics create(final PluginMetrics pluginMetrics) {
        return new OpenSearchSourcePluginMetrics(pluginMetrics);
    }

    private OpenSearchSourcePluginMetrics(final PluginMetrics pluginMetrics) {
        documentsProcessedCounter = pluginMetrics.counter(DOCUMENTS_PROCESSED);
        indicesProcessedCounter = pluginMetrics.counter(INDICES_PROCESSED);
        processingErrorsCounter = pluginMetrics.counter(PROCESSING_ERRORS);
        indexProcessingTimeTimer = pluginMetrics.timer(INDEX_PROCESSING_TIME_ELAPSED);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
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

    public DistributionSummary getBytesReceivedSummary() {
        return bytesReceivedSummary;
    }

    public DistributionSummary getBytesProcessedSummary() {
        return bytesProcessedSummary;
    }
}
