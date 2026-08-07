package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.TestItemInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
public class TokenPaginationCrawlerTest {
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final String INITIAL_TOKEN = "initial-token";
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private TokenCrawlerClient client;
    @Mock
    private PaginationCrawlerWorkerProgressState state;
    @Mock
    private LeaderPartition leaderPartition;
    @Mock
    private Timer partitionWaitTimeTimer;
    @Mock
    private Timer partitionProcessLatencyTimer;
    @Mock
    private Timer crawlingTimer;
    @Mock
    private Counter mockCounter;
    @Mock
    private PluginMetrics mockPluginMetrics;

    private Crawler crawler;
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("CrawlerTest", "crawler");

    @BeforeEach
    public void setup() {
        crawler = new TokenPaginationCrawler(client, pluginMetrics);
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(new TokenPaginationCrawlerLeaderProgressState(INITIAL_TOKEN)));
    }

    @Test
    public void crawlerConstructionTest() {
        reset(leaderPartition);
        assertNotNull(crawler);
    }

    @Test
    public void executePartitionTest() {
        reset(leaderPartition);
        // Mock the getExportStartTime() method to return a valid Instant
        when(state.getExportStartTime()).thenReturn(Instant.now().minusSeconds(1));
        crawler.executePartition(state, buffer, acknowledgementSet);
        verify(client).executePartition(state, buffer, acknowledgementSet);
    }

    @Test
    public void test_metrics_in_crawler() {
        reset(leaderPartition);

        // Mock all required timers and counters for TokenPaginationCrawler constructor
        Timer mockCrawlingTimer = mock(Timer.class);
        Counter mockPartitionsCreatedCounter = mock(Counter.class);
        Counter mockInvalidItemsCounter = mock(Counter.class);

        when(mockPluginMetrics.timer("crawlingTime")).thenReturn(mockCrawlingTimer);
        when(mockPluginMetrics.timer("workerPartitionWaitTime")).thenReturn(partitionWaitTimeTimer);
        when(mockPluginMetrics.timer("workerPartitionProcessLatency")).thenReturn(partitionProcessLatencyTimer);
        when(mockPluginMetrics.counter("paginationWorkerPartitionsCreated")).thenReturn(mockPartitionsCreatedCounter);
        when(mockPluginMetrics.counter("invalidPaginationItems")).thenReturn(mockInvalidItemsCounter);

        TokenPaginationCrawler testCrawler = new TokenPaginationCrawler(client, mockPluginMetrics);

        // Test executePartition method which uses partitionWaitTimeTimer and partitionProcessLatencyTimer
        when(state.getExportStartTime()).thenReturn(Instant.now().minusSeconds(1));

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(partitionProcessLatencyTimer).record(any(Runnable.class));
        doNothing().when(partitionWaitTimeTimer).record(any(Duration.class));

        testCrawler.executePartition(state, buffer, acknowledgementSet);

        // Verify executePartition metrics
        verify(partitionProcessLatencyTimer).record(any(Runnable.class));
        verify(partitionWaitTimeTimer).record(any(Duration.class));

        // Test crawl method which uses crawlingTimer, parititionsCreatedCounter, and invalidPaginationItemsCounter
        // Re-setup leaderPartition mock after reset
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(new TokenPaginationCrawlerLeaderProgressState(INITIAL_TOKEN)));

        List<ItemInfo> itemInfoList = new ArrayList<>();
        itemInfoList.add(null); // This will trigger invalidPaginationItemsCounter
        itemInfoList.add(new TestItemInfo("testId1"));

        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        doNothing().when(mockCrawlingTimer).record(any(Long.class), any());
        doNothing().when(mockPartitionsCreatedCounter).increment();
        doNothing().when(mockInvalidItemsCounter).increment();

        testCrawler.crawl(leaderPartition, coordinator);

        // Verify crawl metrics
        verify(mockCrawlingTimer).record(any(Long.class), any()); // crawlingTimer records crawl duration
        verify(mockPartitionsCreatedCounter).increment(); // parititionsCreatedCounter increments when partition is created
        verify(mockInvalidItemsCounter).increment(); // invalidPaginationItemsCounter increments for null items
    }

    @Test
    void testCrawlWithEmptyList() {
        String lastToken = INITIAL_TOKEN;
        when(client.listItems(lastToken)).thenReturn(Collections.emptyIterator());
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(new TokenPaginationCrawlerLeaderProgressState(lastToken)));
        crawler.crawl(leaderPartition, coordinator);
        verify(coordinator, never()).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNonEmptyList() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        for (int i = 0; i < DEFAULT_BATCH_SIZE; i++) {
            itemInfoList.add(new TestItemInfo("itemId" + i));
        }
        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithMultiplePartitions() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        for (int i = 0; i < DEFAULT_BATCH_SIZE + 1; i++) {
            itemInfoList.add(new TestItemInfo("testId" + i));
        }
        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        verify(coordinator, times(2)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testBatchSize() {  
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = DEFAULT_BATCH_SIZE;
        for (int i = 0; i < maxItemsPerPage; i++) {
            itemInfoList.add(new TestItemInfo("testId" + i));
        }
        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        int expectedNumberOfInvocations = 1;
        verify(coordinator, times(expectedNumberOfInvocations)).createPartition(any(SaasSourcePartition.class));

        List<ItemInfo> itemInfoList2 = new ArrayList<>();
        for (int i = 0; i < maxItemsPerPage * 2; i++) {
            itemInfoList2.add(new TestItemInfo("testId" + i));
        }
        when(client.listItems(anyString())).thenReturn(itemInfoList2.iterator());
        crawler.crawl(leaderPartition, coordinator);
        expectedNumberOfInvocations += 2;
        verify(coordinator, times(expectedNumberOfInvocations)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNullItemsInList() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        itemInfoList.add(null);
        for (int i = 0; i < DEFAULT_BATCH_SIZE - 1; i++) {
            itemInfoList.add(new TestItemInfo("testId" + i));
        }
        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testUpdatingTokenNullMetaData() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        assertEquals("1", ((TokenLeaderProgressState) leaderPartition.getProgressState().get()).getLastToken());
    }

    @Test
    void testUpdatedTokenCreatedNewer() {
        String lastToken = INITIAL_TOKEN;
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems(lastToken)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator);
        assertEquals("1", ((TokenLeaderProgressState) leaderPartition.getProgressState().get()).getLastToken());
    }

    private ItemInfo createTestItemInfo(String id) {
        return new TestItemInfo(id, new HashMap<>(), Instant.now());
    }
}
