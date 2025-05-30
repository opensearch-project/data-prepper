/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.PostgresStreamState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.PostgresSchemaManager;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
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
    private DbTableMetadata dbTableMetadata;

    @Mock
    private LeaderPartition leaderPartition;

    @Mock
    private LeaderProgressState leaderProgressState;

    @Mock
    private PipelineDescription pipelineDescription;

    private String s3Prefix;
    private String pipelineName;
    private LeaderScheduler leaderScheduler;

    @BeforeEach
    void setUp() {
        s3Prefix = UUID.randomUUID().toString();
        leaderScheduler = createObjectUnderTest();
        pipelineName = UUID.randomUUID().toString();

        AwsAuthenticationConfig awsAuthenticationConfig = mock(AwsAuthenticationConfig.class);
        lenient().when(awsAuthenticationConfig.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourceConfig.getAwsAuthenticationConfig()).thenReturn(awsAuthenticationConfig);
        ExportConfig exportConfig = mock(ExportConfig.class);
        lenient().when(exportConfig.getKmsKeyId()).thenReturn(UUID.randomUUID().toString());
        lenient().when(sourceConfig.getExport()).thenReturn(exportConfig);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(pipelineName);
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

    @ParameterizedTest
    @EnumSource(EngineType.class)
    void leader_node_should_perform_init_if_not_initialized(EngineType engineType) throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.of(leaderPartition));
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.isInitialized()).thenReturn(false);
        when(sourceConfig.isExportEnabled()).thenReturn(true);
        when(sourceConfig.getEngine()).thenReturn(engineType);

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

    @Disabled("Flaky test, needs to be fixed")
    @Test
    void test_shutDown() {
        lenient().when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        leaderScheduler.shutdown();
        verifyNoMoreInteractions(sourceCoordinator);
        executorService.shutdownNow();
    }

    @Test
    void leader_node_performs_init_creates_slot_with_expected_name() throws InterruptedException {
        final PostgresSchemaManager postgresSchemaManager = mock(PostgresSchemaManager.class);
        final String pipelineName = "simple-pipeline";

        when(sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).thenReturn(Optional.of(leaderPartition));
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.isInitialized()).thenReturn(false);
        when(sourceConfig.isStreamEnabled()).thenReturn(true);
        when(sourceConfig.getEngine()).thenReturn(EngineType.POSTGRES);

        final LeaderScheduler leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig, s3Prefix,
                postgresSchemaManager, dbTableMetadata, pipelineName);
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        ArgumentCaptor<StreamPartition> streamPartitionArgumentCaptor = ArgumentCaptor.forClass(StreamPartition.class);
        verify(sourceCoordinator).createPartition(any(GlobalState.class));
        verify(sourceCoordinator).createPartition(streamPartitionArgumentCaptor.capture());
        verify(sourceCoordinator).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));

        final StreamPartition streamPartition = streamPartitionArgumentCaptor.getValue();
        assertThat(streamPartition.getProgressState().get().getPostgresStreamState(), notNullValue());

        PostgresStreamState postgresStreamState = streamPartition.getProgressState().get().getPostgresStreamState();
        final String publicationName = postgresStreamState.getPublicationName();
        final String slotName = postgresStreamState.getReplicationSlotName();
        assertThat(publicationName, startsWith("data_prepper_simple_pipeline"));
        assertThat(slotName, startsWith("data_prepper_simple_pipeline"));
        assertThat(publicationName.substring(publicationName.length() - 8), is(slotName.substring(slotName.length() - 8)));
    }

    private LeaderScheduler createObjectUnderTest() {
        return new LeaderScheduler(sourceCoordinator, sourceConfig, s3Prefix, schemaManager, dbTableMetadata, pipelineName);
    }
}