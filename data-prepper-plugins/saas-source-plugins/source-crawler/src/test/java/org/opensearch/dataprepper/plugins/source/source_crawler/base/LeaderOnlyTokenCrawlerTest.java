package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.TestItemInfo;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;

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
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.doNothing;
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
    @Mock
    private PaginationCrawlerWorkerProgressState workerState;

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
    void testRetryPartitionStateSerializationAndDeserialization() {
        List<ItemInfo> batch = createTestItems( 1);

        // Mock client to return our test items
        when(client.listItems(INITIAL_TOKEN)).thenReturn(batch.iterator());

        // Capture the created partition
        ArgumentCaptor<SaasSourcePartition> partitionCaptor = ArgumentCaptor.forClass(SaasSourcePartition.class);

        // Simulate negative acknowledgment
        when(acknowledgementSetManager.create(any(), any())).thenAnswer(inv -> {
            Consumer<Boolean> callback = inv.getArgument(0);
            callback.accept(false);
            return acknowledgementSet;
        });

        // Create retry partition
        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        // Verify partition creation and get original state
        verify(coordinator).createPartition(partitionCaptor.capture());
        SaasSourcePartition originalPartition = partitionCaptor.getValue();
        PaginationCrawlerWorkerProgressState originalState =
                (PaginationCrawlerWorkerProgressState) originalPartition.getProgressState().get();

        // Test serialization/deserialization
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());  // For Instant serialization

        try {
            // Serialize state
            String serializedState = objectMapper.writeValueAsString(originalState);

            // Deserialize state
            PaginationCrawlerWorkerProgressState deserializedState =
                    objectMapper.readValue(serializedState, PaginationCrawlerWorkerProgressState.class);

            // Verify deserialized state matches original
            assertEquals(originalState.getItemIds(), deserializedState.getItemIds());
            assertEquals(originalState.getKeyAttributes(), deserializedState.getKeyAttributes());
            assertEquals(originalState.getExportStartTime(), deserializedState.getExportStartTime());
            assertEquals(originalState.getLoadedItems(), deserializedState.getLoadedItems());
        } catch (JsonProcessingException e) {
            fail("Serialization/deserialization failed", e);
        }
    }

    @Test
    void testNegativeAcknowledgment() {
        List<ItemInfo> items = createTestItems(1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());

        // Setup immediate negative acknowledgment
        doAnswer(invocation -> {
            Consumer<Boolean> callback = invocation.getArgument(0);
            callback.accept(false);  // Trigger negative ack immediately
            return acknowledgementSet;
        }).when(acknowledgementSetManager).create(any(), eq(TEST_TIMEOUT));

        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        // Verify behavior
        verify(client, times(1)).writeBatchToBuffer(any(), any(), any());
        verify(coordinator, times(1)).createPartition(any());
        verify(acknowledgementSet, times(1)).complete();
    }


    @Test
    void testAcknowledgmentTimeout() {
        List<ItemInfo> items = createTestItems( 1);
        when(client.listItems(INITIAL_TOKEN)).thenReturn(items.iterator());
        when(acknowledgementSetManager.create(any(), eq(TEST_TIMEOUT)))
                .thenReturn(acknowledgementSet);

        ArgumentCaptor<Consumer<Boolean>> callbackCaptor = ArgumentCaptor.forClass(Consumer.class);

        crawler.setAcknowledgementsEnabled(true);
        crawler.crawl(leaderPartition, coordinator);

        verify(acknowledgementSetManager).create(callbackCaptor.capture(), eq(TEST_TIMEOUT));

        // Verify timeout behavior
        verify(client, times(1)).writeBatchToBuffer(any(), any(), any());
        verify(acknowledgementSet).complete();
        verify(coordinator).createPartition(any());
    }

    @Test
    public void testExecutePartitionMetrics() {
        reset(leaderPartition);

        // mock timers and counters
        Timer mockCrawlingTimer = mock(Timer.class);
        Timer partitionWaitTimeTimer = mock(Timer.class);
        Timer partitionProcessLatencyTimer = mock(Timer.class);
        Timer mockBufferWriteTimer = mock(Timer.class);
        Counter mockBatchesFailedCounter = mock(Counter.class);
        Counter mockAcknowledgementSetSuccesses = mock(Counter.class);
        Counter mockAcknowledgementSetFailures = mock(Counter.class);

        // setup mock plugin metrics
        PluginMetrics mockPluginMetrics = mock(PluginMetrics.class);
        when(mockPluginMetrics.timer("crawlingTime")).thenReturn(mockCrawlingTimer);
        when(mockPluginMetrics.timer("workerPartitionWaitTime")).thenReturn(partitionWaitTimeTimer);
        when(mockPluginMetrics.timer("workerPartitionProcessLatency")).thenReturn(partitionProcessLatencyTimer);
        when(mockPluginMetrics.timer("bufferWriteTime")).thenReturn(mockBufferWriteTimer);
        when(mockPluginMetrics.counter("batchesFailed")).thenReturn(mockBatchesFailedCounter);
        when(mockPluginMetrics.counter("acknowledgementSetSuccesses")).thenReturn(mockAcknowledgementSetSuccesses);
        when(mockPluginMetrics.counter("acknowledgementSetFailures")).thenReturn(mockAcknowledgementSetFailures);

        LeaderOnlyTokenCrawler testCrawler = new LeaderOnlyTokenCrawler(client, mockPluginMetrics);

        // test executePartition with metrics
        when(workerState.getExportStartTime()).thenReturn(Instant.now().minusSeconds(1));

        // make latency timer execute the runnable so client.executePartition() gets called
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(partitionProcessLatencyTimer).record(any(Runnable.class));
        
        doNothing().when(partitionWaitTimeTimer).record(any(Duration.class));

        testCrawler.executePartition(workerState, buffer, acknowledgementSet);

        // verify metrics are recorded
        verify(partitionProcessLatencyTimer).record(any(Runnable.class));
        verify(partitionWaitTimeTimer).record(any(Duration.class));
        verify(client).executePartition(workerState, buffer, acknowledgementSet);
    }

    private List<ItemInfo> createTestItems(int count) {
        List<ItemInfo> items = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            items.add(new TestItemInfo("id" + i, new HashMap<>(), Instant.now()));
        }
        return items;
    }
}
