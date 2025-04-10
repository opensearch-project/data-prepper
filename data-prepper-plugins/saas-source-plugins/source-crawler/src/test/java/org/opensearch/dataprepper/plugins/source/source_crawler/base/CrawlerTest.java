package org.opensearch.dataprepper.plugins.source.source_crawler.base;

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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.TestItemInfo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
public class CrawlerTest {
    private static final int DEFAULT_BATCH_SIZE = 50;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private CrawlerClient client;
    @Mock
    private SaasWorkerProgressState state;
    @Mock
    private LeaderPartition leaderPartition;
    private Crawler crawler;
    private final Instant lastPollTime = Instant.ofEpochMilli(0);
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("CrawlerTest", "crawler");

    @BeforeEach
    public void setup() {
        crawler = new Crawler(client, pluginMetrics);
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(new LeaderProgressState(lastPollTime)));
    }

    @Test
    public void crawlerConstructionTest() {
        reset(leaderPartition);
        assertNotNull(crawler);
    }

    @Test
    public void executePartitionTest() {
        reset(leaderPartition);
        crawler.executePartition(state, buffer, acknowledgementSet);
        verify(client).executePartition(state, buffer, acknowledgementSet);
    }

    @Test
    void testCrawlWithEmptyList() {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        when(client.listItems(lastPollTime)).thenReturn(Collections.emptyIterator());
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(new LeaderProgressState(lastPollTime)));
        crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        verify(coordinator, never()).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNonEmptyList() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        for (int i = 0; i < DEFAULT_BATCH_SIZE; i++) {
            itemInfoList.add(new TestItemInfo("itemId"));
        }
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));

    }

    @Test
    void testCrawlWithMultiplePartitions() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        for (int i = 0; i < DEFAULT_BATCH_SIZE + 1; i++) {
            itemInfoList.add(new TestItemInfo("testId"));
        }
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        verify(coordinator, times(2)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testBatchSize() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        int maxItemsPerPage = 50;
        for (int i = 0; i < maxItemsPerPage; i++) {
            itemInfoList.add(new TestItemInfo("testId"));
        }
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator, maxItemsPerPage);
        int expectedNumberOfInvocations = 1;
        verify(coordinator, times(expectedNumberOfInvocations)).createPartition(any(SaasSourcePartition.class));

        List<ItemInfo> itemInfoList2 = new ArrayList<>();
        int maxItemsPerPage2 = 25;
        for (int i = 0; i < maxItemsPerPage; i++) {
            itemInfoList2.add(new TestItemInfo("testId"));
        }
        when(client.listItems(any())).thenReturn(itemInfoList2.iterator());
        crawler.crawl(leaderPartition, coordinator, maxItemsPerPage2);
        expectedNumberOfInvocations += 2;
        verify(coordinator, times(expectedNumberOfInvocations)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testCrawlWithNullItemsInList() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        itemInfoList.add(null);
        for (int i = 0; i < DEFAULT_BATCH_SIZE - 1; i++) {
            itemInfoList.add(new TestItemInfo("testId"));
        }
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
    }

    @Test
    void testUpdatingPollTimeNullMetaData() {
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        Instant updatedPollTime = crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        assertNotEquals(Instant.ofEpochMilli(0), updatedPollTime);
    }

    @Test
    void testUpdatedPollTimeNiCreatedLarger() {
        Instant lastPollTime = Instant.ofEpochMilli(0);
        List<ItemInfo> itemInfoList = new ArrayList<>();
        ItemInfo testItem = createTestItemInfo("1");
        itemInfoList.add(testItem);
        when(client.listItems(lastPollTime)).thenReturn(itemInfoList.iterator());
        Instant updatedPollTime = crawler.crawl(leaderPartition, coordinator, DEFAULT_BATCH_SIZE);
        assertNotEquals(lastPollTime, updatedPollTime);
    }

    private ItemInfo createTestItemInfo(String id) {
        return new TestItemInfo(id, new HashMap<>(), Instant.now());
    }

}