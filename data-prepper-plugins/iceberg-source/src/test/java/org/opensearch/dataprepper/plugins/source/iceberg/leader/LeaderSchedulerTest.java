/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.leader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;

import java.time.Duration;
import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler.DATA_FILES_PER_TASK;
import static org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler.DATA_FILE_BYTES;
import static org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler.SNAPSHOTS_PROCESSED_COUNT;

@ExtendWith(MockitoExtension.class)
class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter snapshotsProcessedCounter;
    @Mock
    private DistributionSummary dataFileBytesSummary;
    @Mock
    private DistributionSummary dataFilesPerTaskSummary;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(SNAPSHOTS_PROCESSED_COUNT)).thenReturn(snapshotsProcessedCounter);
        when(pluginMetrics.summary(DATA_FILE_BYTES)).thenReturn(dataFileBytesSummary);
        when(pluginMetrics.summary(DATA_FILES_PER_TASK)).thenReturn(dataFilesPerTaskSummary);
    }

    @Test
    void constructor_registersAllMetrics() {
        new LeaderScheduler(sourceCoordinator, Map.of(), Duration.ofSeconds(5), Map.of(), pluginMetrics);

        verify(pluginMetrics).counter(SNAPSHOTS_PROCESSED_COUNT);
        verify(pluginMetrics).summary(DATA_FILE_BYTES);
        verify(pluginMetrics).summary(DATA_FILES_PER_TASK);
    }
}
