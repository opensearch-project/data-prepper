/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResyncSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private ResyncPartition resyncPartition;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private QueryManager queryManager;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private DbTableMetadata dbTableMetadata;

    private String s3Prefix;
    private ExecutorService resyncExecutor;
    private ResyncScheduler resyncScheduler;


    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        resyncScheduler = createObjectUnderTest();
    }

    @Test
    void test_run_then_complete_partition() {
        final String dbIdentifier = UUID.randomUUID().toString();
        final GlobalState globalState = mock(GlobalState.class);
        final Map<String, Object> progressState = mock(Map.class);
        when(sourceCoordinator.acquireAvailablePartition(ResyncPartition.PARTITION_TYPE)).thenReturn(Optional.of(resyncPartition));
        when(sourceConfig.getDbIdentifier()).thenReturn(dbIdentifier);
        when(sourceCoordinator.getPartition(dbIdentifier)).thenReturn(Optional.of(globalState));
        when(globalState.getProgressState()).thenReturn(Optional.of(progressState));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final MySQLResyncWorker resyncWorker = mock(MySQLResyncWorker.class);
        doNothing().when(resyncWorker).run();

        executorService.submit(() -> {
            try (MockedStatic<MySQLResyncWorker> resyncWorkerMockedStatic = mockStatic(MySQLResyncWorker.class);
                 MockedStatic<DbTableMetadata> dbTableMetadataMockedStatic = mockStatic(DbTableMetadata.class)) {
                dbTableMetadataMockedStatic.when(() -> DbTableMetadata.fromMap(progressState)).thenReturn(dbTableMetadata);
                resyncWorkerMockedStatic.when(() -> MySQLResyncWorker.create(eq(resyncPartition), eq(sourceConfig),
                        eq(queryManager), eq(buffer), any(RecordConverter.class), any(), eq(dbTableMetadata))).thenReturn(resyncWorker);
                resyncScheduler.run();
            }
        });
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ResyncPartition.PARTITION_TYPE));
        executorService.shutdownNow();

        verify(sourceCoordinator).completePartition(resyncPartition);
    }

    private ResyncScheduler createObjectUnderTest() {
        return new ResyncScheduler(sourceCoordinator, sourceConfig, queryManager, s3Prefix, buffer, pluginMetrics, acknowledgementSetManager);
    }

}