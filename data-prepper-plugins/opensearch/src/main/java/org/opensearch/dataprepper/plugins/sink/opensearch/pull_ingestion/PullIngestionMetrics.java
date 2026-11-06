/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull_ingestion;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;

public class PullIngestionMetrics {
    static final String DOCUMENTS_SUCCEEDED = "pullIngestionDocumentsSucceeded";
    static final String DOCUMENTS_FAILED = "pullIngestionDocumentsFailed";
    static final String LATENCY = "pullIngestionLatency";
    static final String BYTES = "pullIngestionBytes";

    private final Counter documentsSucceeded;
    private final Counter documentsFailed;
    private final Timer latency;
    private final DistributionSummary bytes;

    public PullIngestionMetrics(final PluginMetrics pluginMetrics) {
        this.documentsSucceeded = pluginMetrics.counter(DOCUMENTS_SUCCEEDED);
        this.documentsFailed = pluginMetrics.counter(DOCUMENTS_FAILED);
        this.latency = pluginMetrics.timer(LATENCY);
        this.bytes = pluginMetrics.summary(BYTES);
    }

    public void incrementDocumentsSucceeded() {
        documentsSucceeded.increment();
    }

    public void incrementDocumentsFailed() {
        documentsFailed.increment();
    }

    public void recordLatency(final Runnable flush) {
        latency.record(flush);
    }

    public void recordBytes(final long byteCount) {
        bytes.record(byteCount);
    }
}
