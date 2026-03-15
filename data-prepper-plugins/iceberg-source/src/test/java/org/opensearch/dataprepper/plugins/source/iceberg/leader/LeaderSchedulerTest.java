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
import static org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler.SNAPSHOTS_PROCESSED_COUNT;

@ExtendWith(MockitoExtension.class)
class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter snapshotsProcessedCounter;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(SNAPSHOTS_PROCESSED_COUNT)).thenReturn(snapshotsProcessedCounter);
    }

    @Test
    void constructor_registersSnapshotsProcessedMetric() {
        new LeaderScheduler(sourceCoordinator, Map.of(), Duration.ofSeconds(5), Map.of(), pluginMetrics);

        verify(pluginMetrics).counter(SNAPSHOTS_PROCESSED_COUNT);
    }
}
