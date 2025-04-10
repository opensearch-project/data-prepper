package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LeaderSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private Crawler crawler;
    private final int batchSize = 50;

    @Test
    void testUnableToAcquireLeaderPartition() throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler, batchSize);
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        Thread.sleep(100);
        executorService.shutdownNow();
        verifyNoInteractions(crawler);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLeaderPartitionsCreation(boolean initializationState) throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler, batchSize);
        LeaderPartition leaderPartition = new LeaderPartition();
        leaderPartition.getProgressState().get().setInitialized(initializationState);
        leaderPartition.getProgressState().get().setLastPollTime(Instant.ofEpochMilli(0L));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        doThrow(RuntimeException.class).when(coordinator).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, times(1)).crawl(leaderPartition, coordinator, batchSize);
        verify(coordinator, times(1)).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));

    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionWhileAcquiringLeaderPartition(boolean initializationState) throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler, batchSize);
        LeaderPartition leaderPartition = new LeaderPartition();
        leaderPartition.getProgressState().get().setInitialized(initializationState);
        leaderPartition.getProgressState().get().setLastPollTime(Instant.ofEpochMilli(0L));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verifyNoInteractions(crawler);
    }

    @Test
    void testWhileLoopRunnningAfterTheSleep() throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler, batchSize);
        leaderScheduler.setLeaseInterval(Duration.ofMillis(10));
        LeaderPartition leaderPartition = new LeaderPartition();
        leaderPartition.getProgressState().get().setInitialized(false);
        leaderPartition.getProgressState().get().setLastPollTime(Instant.ofEpochMilli(0L));
        when(crawler.crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class), anyInt())).thenReturn(Instant.ofEpochMilli(10));
        when(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(leaderPartition))
                .thenThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, atLeast(2)).crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class), anyInt());
    }
}
