package org.opensearch.dataprepper.plugins.mongo.leader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.LeaderPartition;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.mongo.leader.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

@ExtendWith(MockitoExtension.class)
public class LeaderSchedulerTest {
    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private CollectionConfig collectionConfig;

    @Mock
    private CollectionConfig.ExportConfig exportConfig;

    private LeaderScheduler leaderScheduler;
    private LeaderPartition leaderPartition;

    @Test
    void test_non_leader_run() {
        leaderScheduler = new LeaderScheduler(coordinator, List.of(collectionConfig), Duration.ofMillis(100));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> leaderScheduler.run());
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> verifyNoInteractions(collectionConfig));
        executorService.shutdownNow();
    }

    @Test
    void test_should_init() {

        leaderScheduler = new LeaderScheduler(coordinator, List.of(collectionConfig), Duration.ofMillis(100));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        given(collectionConfig.isExportRequired()).willReturn(true);
        given(collectionConfig.isStreamRequired()).willReturn(true);
        given(collectionConfig.getExportConfig()).willReturn(exportConfig);
        given(exportConfig.getItemsPerPartition()).willReturn(new Random().nextInt());
        given(collectionConfig.getCollection()).willReturn(UUID.randomUUID().toString());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> leaderScheduler.run());

        // Acquire the init partition
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> verify(coordinator, atLeast(1)).acquireAvailablePartition(eq(LeaderPartition.PARTITION_TYPE)));

        future.cancel(true);

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(coordinator).giveUpPartition(leaderPartition));

        // Should create 1 export partition + 1 stream partitions + 1 global table state
        verify(coordinator, times(3)).createPartition(any(EnhancedSourcePartition.class));
        verify(coordinator, atLeast(1)).saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(true));
        executorService.shutdownNow();
    }

    @Test
    void test_should_init_export() {

        leaderScheduler = new LeaderScheduler(coordinator, List.of(collectionConfig), Duration.ofMillis(100));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        given(collectionConfig.isExportRequired()).willReturn(true);
        given(collectionConfig.getExportConfig()).willReturn(exportConfig);
        given(exportConfig.getItemsPerPartition()).willReturn(new Random().nextInt());
        given(collectionConfig.getCollection()).willReturn(UUID.randomUUID().toString());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> leaderScheduler.run());

        // Acquire the init partition
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(coordinator, atLeast(1)).acquireAvailablePartition(eq(LeaderPartition.PARTITION_TYPE)));

        future.cancel(true);

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(coordinator).giveUpPartition(leaderPartition));

        // Should create 1 export partition + 1 stream partitions + 1 global table state
        verify(coordinator, times(2)).createPartition(any(EnhancedSourcePartition.class));
        verify(coordinator, atLeast(1)).saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(true));
        executorService.shutdownNow();
    }

    @Test
    void test_should_init_stream() {

        leaderScheduler = new LeaderScheduler(coordinator, List.of(collectionConfig), Duration.ofMillis(100));
        leaderPartition = new LeaderPartition();
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        given(collectionConfig.isStreamRequired()).willReturn(true);
        given(collectionConfig.getCollection()).willReturn(UUID.randomUUID().toString());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> leaderScheduler.run());
        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> verify(coordinator, atLeast(1)).acquireAvailablePartition(eq(LeaderPartition.PARTITION_TYPE)));

        future.cancel(true);

        await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->  verify(coordinator).giveUpPartition(leaderPartition));

        // Should create 1 export partition + 1 stream partitions + 1 global table state
        verify(coordinator, times(2)).createPartition(any(EnhancedSourcePartition.class));
        verify(coordinator, atLeast(1)).saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));

        assertThat(leaderPartition.getProgressState().get().isInitialized(), equalTo(true));
        executorService.shutdownNow();
    }
}
