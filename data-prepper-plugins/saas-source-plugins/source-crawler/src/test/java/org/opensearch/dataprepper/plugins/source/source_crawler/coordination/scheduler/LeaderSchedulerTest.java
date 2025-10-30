package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
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

    private static String LAST_TOKEN = "sample-token-123";

    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private Crawler crawler;

    @Test
    void testUnableToAcquireLeaderPartition() throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.empty());

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);
        Thread.sleep(100);
        executorService.shutdownNow();
        verifyNoInteractions(crawler);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTokenPaginationCrawlerLeaderPartitionsCreation(boolean initializationState) throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        LeaderPartition leaderPartition = new LeaderPartition(new TokenPaginationCrawlerLeaderProgressState(""));
        TokenPaginationCrawlerLeaderProgressState state = (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(initializationState);
        state.setLastToken(LAST_TOKEN);
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        doThrow(RuntimeException.class).when(coordinator).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, times(1)).crawl(leaderPartition, coordinator);
        verify(coordinator, times(1)).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));

    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPaginationCrawlerLeaderPartitionsCreation(boolean initializationState) throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        LeaderPartition leaderPartition = new LeaderPartition(new PaginationCrawlerLeaderProgressState(Instant.EPOCH));
        PaginationCrawlerLeaderProgressState state = (PaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(initializationState);
        state.setLastPollTime(Instant.ofEpochMilli(0L));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willReturn(Optional.of(leaderPartition));
        doThrow(RuntimeException.class).when(coordinator).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, times(1)).crawl(leaderPartition, coordinator);
        verify(coordinator, times(1)).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));

    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExceptionWhileAcquiringLeaderPartition(boolean initializationState) throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        LeaderPartition leaderPartition = new LeaderPartition(new PaginationCrawlerLeaderProgressState(Instant.EPOCH));
        PaginationCrawlerLeaderProgressState state = (PaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(initializationState);
        state.setLastPollTime(Instant.ofEpochMilli(0L));
        given(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verifyNoInteractions(crawler);
    }

    @Test
    void testTokenPaginationCrawlerWhileLoopRunnningAfterTheSleep() throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        leaderScheduler.setLeaseInterval(Duration.ofMillis(10));
        LeaderPartition leaderPartition = new LeaderPartition(new TokenPaginationCrawlerLeaderProgressState(""));
        TokenPaginationCrawlerLeaderProgressState state = (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(false);
        state.setLastToken(LAST_TOKEN);
        when(crawler.crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class))).thenReturn(Instant.ofEpochMilli(10));
        when(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(leaderPartition))
                .thenThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, atLeast(2)).crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class));
    }

    @Test
    void testWhileLoopRunnningAfterTheSleep() throws InterruptedException {
        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        leaderScheduler.setLeaseInterval(Duration.ofMillis(10));
        LeaderPartition leaderPartition = new LeaderPartition(new PaginationCrawlerLeaderProgressState(Instant.EPOCH));
        PaginationCrawlerLeaderProgressState state = (PaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(false);
        state.setLastPollTime(Instant.ofEpochMilli(0L));
        when(crawler.crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class))).thenReturn(Instant.ofEpochMilli(10));
        when(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(leaderPartition))
                .thenThrow(RuntimeException.class);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        //Wait for more than a minute as the default while loop wait time in leader scheduler is 1 minute
        Thread.sleep(100);
        executorService.shutdownNow();

        // Check if crawler was invoked and updated leader lease renewal time
        verify(crawler, atLeast(2)).crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class));
    }

    @Test
    void testSetLeaderProgressState_throwsExceptionOnStateMismatch() {
        // Create a LeaderPartition with TokenPaginationCrawlerLeaderProgressState
        LeaderPartition leaderPartition = new LeaderPartition(new TokenPaginationCrawlerLeaderProgressState(""));

        // Try to set a different type of state (PaginationCrawlerLeaderProgressState)
        PaginationCrawlerLeaderProgressState incompatibleState = new PaginationCrawlerLeaderProgressState(Instant.now());

        // Verify that RuntimeException is thrown due to state type mismatch
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            leaderPartition.setLeaderProgressState(incompatibleState);
        });
    }

    @Test
    @DisplayName("Ensure that if DynamoDB becomes unreachable, the leader gives up the partition and retries acquisition")
    void testLeaderPartitionGivenUpOnSaveFailure_andRetryAcquire() throws InterruptedException {
        // This test verifies the fix for line 72: leaderPartition = null when saveProgressStateForPartition fails
        // This ensures that if DynamoDB becomes unreachable, the leader gives up the partition and retries acquisition

        LeaderScheduler leaderScheduler = new LeaderScheduler(coordinator, crawler);
        leaderScheduler.setLeaseInterval(Duration.ofMillis(10));

        LeaderPartition leaderPartition = new LeaderPartition(new TokenPaginationCrawlerLeaderProgressState(""));
        TokenPaginationCrawlerLeaderProgressState state = (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        state.setInitialized(true);
        state.setLastToken(LAST_TOKEN);

        // First acquisition succeeds, but subsequent saveProgressStateForPartition fails (simulating DynamoDB outage)
        // Then second acquisition should be attempted after giving up the partition
        when(coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE))
                .thenReturn(Optional.of(leaderPartition))  // First acquisition succeeds
                .thenReturn(Optional.of(leaderPartition)); // Second acquisition after giving up partition

        // saveProgressStateForPartition throws exception to simulate DynamoDB outage
        doThrow(new RuntimeException("DynamoDB unreachable"))
                .doNothing() // Second time succeeds after recovery
                .when(coordinator).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(leaderScheduler);

        // Wait long enough for multiple iterations
        Thread.sleep(100);
        executorService.shutdownNow();

        // Verify that crawler was called multiple times (showing the scheduler continued to work)
        verify(crawler, atLeast(2)).crawl(any(LeaderPartition.class), any(EnhancedSourceCoordinator.class));

        // Verify that acquireAvailablePartition was called at least twice:
        // once for initial acquisition, and again after giving up due to save failure
        verify(coordinator, atLeast(2)).acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);

        // Verify that saveProgressStateForPartition was attempted multiple times
        verify(coordinator, atLeast(2)).saveProgressStateForPartition(any(LeaderPartition.class), any(Duration.class));
    }
}
