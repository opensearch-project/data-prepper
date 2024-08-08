/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock(answer = Answers.RETURNS_DEFAULTS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private DbMetadata dbMetadata;

    @Mock
    private LeaderPartition leaderPartition;

    @Mock
    private LeaderProgressState leaderProgressState;

    private LeaderScheduler leaderScheduler;

    @BeforeEach
    void setUp() {
        leaderScheduler = createObjectUnderTest();

        AwsAuthenticationConfig awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        lenient().when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        ExportConfig exportConfig = mock(ExportConfig.class);
        lenient().when(exportConfig.getKmsKeyId()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourceConfig.getExport()).thenReturn(exportConfig);
    }

    @Test
    void non_leader_node_should_not_perform_init() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(sourceCoordinator, never()).createPartition(any(GlobalState.class));
        verify(sourceCoordinator, never()).createPartition(any(ExportPartition.class));
    }

    @Test
    void leader_node_should_perform_init_if_not_initialized() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.of(leaderPartition));
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.isInitialized()).thenReturn(false);
        when(sourceConfig.isExportEnabled()).thenReturn(true);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(sourceCoordinator).createPartition(any(GlobalState.class));
        verify(sourceCoordinator).createPartition(any(ExportPartition.class));
        verify(sourceCoordinator).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
    }

    @Test
    void leader_node_should_skip_init_if_initialized() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.of(leaderPartition));
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.isInitialized()).thenReturn(true);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(sourceCoordinator, never()).createPartition(any(GlobalState.class));
        verify(sourceCoordinator, never()).createPartition(any(ExportPartition.class));
        verify(sourceCoordinator).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
    }

    @Test
    void test_shutDown() {
        lenient().when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        leaderScheduler.shutdown();
        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    private LeaderScheduler createObjectUnderTest() {
        return new LeaderScheduler(sourceCoordinator, sourceConfig, schemaManager, dbMetadata);
    }
}