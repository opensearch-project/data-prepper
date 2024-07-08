/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import software.amazon.awssdk.services.rds.RdsClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RdsServiceTest {

    @Mock
    private RdsClient rdsClient;

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private ExecutorService executor;

    @Mock
    private EventFactory eventFactory;

    @Mock
    private ClientFactory clientFactory;

    @Mock
    private Buffer<Record<Event>> buffer;

    @BeforeEach
    void setUp() {
        when(clientFactory.buildRdsClient()).thenReturn(rdsClient);
    }

    @Test
    void test_normal_service_start() {
        RdsService rdsService = createObjectUnderTest();
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            rdsService.start(buffer);
        }

        verify(executor).submit(any(LeaderScheduler.class));
        verify(executor).submit(any(ExportScheduler.class));
    }

    @Test
    void test_service_shutdown_calls_executor_shutdownNow() {
        RdsService rdsService = createObjectUnderTest();
        try (final MockedStatic<Executors> executorsMockedStatic = mockStatic(Executors.class)) {
            executorsMockedStatic.when(() -> Executors.newFixedThreadPool(anyInt())).thenReturn(executor);
            rdsService.start(buffer);
        }
        rdsService.shutdown();

        verify(executor).shutdownNow();
    }

    private RdsService createObjectUnderTest() {
        return new RdsService(sourceCoordinator, sourceConfig, eventFactory, clientFactory, pluginMetrics);
    }
}