package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
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
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
class LeaderOnlyTokenCrawlerTest {
    private static final int BATCH_SIZE = 1000;
    private static final String INITIAL_TOKEN = "initial-token";
    private static final Duration TEST_TIMEOUT = Duration.ofMillis(100);

    @Mock
    private LeaderOnlyTokenCrawlerClient client;
    @Mock
    private EnhancedSourceCoordinator coordinator;
    @Mock
    private LeaderPartition leaderPartition;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private Buffer<Record<Event>> buffer;

    private LeaderOnlyTokenCrawler crawler;
    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("CrawlerTest", "crawler");

    @BeforeEach
    void setup() {
        crawler = new LeaderOnlyTokenCrawler(client, pluginMetrics);
        crawler.setAcknowledgementSetManager(acknowledgementSetManager);
        crawler.setBuffer(buffer);
        crawler.setNoAckTimeout(TEST_TIMEOUT);  // Set short timeout for tests
        when(leaderPartition.getProgressState())
                .thenReturn(Optional.of(new TokenPaginationCrawlerLeaderProgressState(INITIAL_TOKEN)));
    }

    @Test
    void testCrawlWithEmptyList() {
        when(client.listItems(INITIAL_TOKEN)).thenReturn(Collections.emptyIterator());
        crawler.crawl(leaderPartition, coordinator);
        verify(client, never()).writeBatchToBuffer(any(), any(), any());
    }

    @Test
    void testSimpleCrawl() {
        List<ItemInfo> items = createTestItems(1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        crawler.crawl(leaderPartition, coordinator);
        verify(client).writeBatchToBuffer(items, buffer, null);
    }

    @Test
    void testCrawlWithAcknowledgmentsEnabled() {
        List<ItemInfo> items = createTestItems(1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        when(acknowledgementSetManager.create(any(), eq(TEST_TIMEOUT)))
                .thenReturn(acknowledgementSet);

        ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        verify(acknowledgementSetManager).create(callbackCaptor.capture(), eq(TEST_TIMEOUT));

        // Simulate immediate successful acknowledgment
        callbackCaptor.getValue().accept(true);

        verify(client).writeBatchToBuffer(items, buffer, acknowledgementSet);
        verify(acknowledgementSet).complete();
        verify(coordinator).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
    }

    @Test
    void testCrawlWithMultipleBatches() {
        List<ItemInfo> items = createTestItems(BATCH_SIZE + 1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());

        crawler.setAcknowledgementsEnabled(false);
        crawler.crawl(leaderPartition, coordinator);

        verify(client, times(2)).writeBatchToBuffer(any(), eq(buffer), any());
    }

    @Test
    void testBufferWriteFailure() {
        List<ItemInfo> items = createTestItems(1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        doThrow(new RuntimeException("Buffer write failed"))
                .when(client).writeBatchToBuffer(any(), any(), any());

        assertThrows(RuntimeException.class, () ->
                crawler.crawl(leaderPartition, coordinator));
    }

    @Test
    void testProgressStateUpdate() {
        List<ItemInfo> items = createTestItems(1);
        String newToken = "id0";
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());

        crawler.setAcknowledgementsEnabled(false);
        crawler.crawl(leaderPartition, coordinator);

        verify(coordinator, times(2)).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
        TokenPaginationCrawlerLeaderProgressState state =
                (TokenPaginationCrawlerLeaderProgressState) leaderPartition.getProgressState().get();
        assertEquals(newToken, state.getLastToken());
    }

    @Test
    void testNegativeAcknowledgment() {
        List<ItemInfo> items = createTestItems(BATCH_SIZE + 1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        when(acknowledgementSetManager.create(any(), eq(TEST_TIMEOUT)))
                .thenReturn(acknowledgementSet);

        ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        verify(acknowledgementSetManager).create(callbackCaptor.capture(), eq(TEST_TIMEOUT));

        // Simulate negative acknowledgment
        callbackCaptor.getValue().accept(false);

        verify(client, times(1)).writeBatchToBuffer(any(), any(), any());
        verify(coordinator, never()).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
    }

    @Test
    void testAcknowledgmentTimeout() {
        List<ItemInfo> items = createTestItems(BATCH_SIZE + 1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        when(acknowledgementSetManager.create(any(), eq(TEST_TIMEOUT)))
                .thenReturn(acknowledgementSet);

        ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        verify(acknowledgementSetManager).create(callbackCaptor.capture(), eq(TEST_TIMEOUT));

        // Verify:
        // 1. Only first batch was processed
        verify(client, times(1)).writeBatchToBuffer(any(), any(), any());
        // 2. No checkpoint update happened
        verify(coordinator, never()).saveProgressStateForPartition(eq(leaderPartition), any(Duration.class));
        // 3. Acknowledgment set was completed
        verify(acknowledgementSet).complete();
    }


    private List<ItemInfo> createTestItems(int count) {
        List<ItemInfo> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            items.add(new TestItemInfo("id" + i, new HashMap<>(), Instant.now()));
        }
        return items;
    }
}