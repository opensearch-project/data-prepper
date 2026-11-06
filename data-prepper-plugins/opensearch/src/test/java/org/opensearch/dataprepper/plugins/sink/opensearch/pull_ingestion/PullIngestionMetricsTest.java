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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.concurrent.ThreadLocalRandom;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PullIngestionMetricsTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter documentsSucceededCounter;

    @Mock
    private Counter documentsFailedCounter;

    @Mock
    private Timer latencyTimer;

    @Mock
    private DistributionSummary bytesSummary;

    private PullIngestionMetrics objectUnderTest;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(PullIngestionMetrics.DOCUMENTS_SUCCEEDED)).thenReturn(documentsSucceededCounter);
        when(pluginMetrics.counter(PullIngestionMetrics.DOCUMENTS_FAILED)).thenReturn(documentsFailedCounter);
        when(pluginMetrics.timer(PullIngestionMetrics.LATENCY)).thenReturn(latencyTimer);
        when(pluginMetrics.summary(PullIngestionMetrics.BYTES)).thenReturn(bytesSummary);

        objectUnderTest = new PullIngestionMetrics(pluginMetrics);
    }

    @Test
    void incrementDocumentsSucceeded_increments_counter() {
        objectUnderTest.incrementDocumentsSucceeded();

        verify(documentsSucceededCounter).increment();
    }

    @Test
    void incrementDocumentsFailed_increments_counter() {
        objectUnderTest.incrementDocumentsFailed();

        verify(documentsFailedCounter).increment();
    }

    @Test
    void recordLatency_records_with_timer() {
        final Runnable runnable = () -> {};

        objectUnderTest.recordLatency(runnable);

        verify(latencyTimer).record(runnable);
    }

    @Test
    void recordBytes_records_with_summary() {
        final long byteCount = ThreadLocalRandom.current().nextLong(1, 100000);

        objectUnderTest.recordBytes(byteCount);

        verify(bytesSummary).record(byteCount);
    }
}
