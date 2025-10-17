package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DimensionalTimeSliceCrawlerTest {

    @Mock
    private CrawlerClient client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter partitionsCreatedCounter;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @InjectMocks
    private DimensionalTimeSliceCrawler crawler;

    @Captor
    private ArgumentCaptor<SaasSourcePartition> partitionCaptor;

    private static final List<String> LOG_TYPES = Arrays.asList("Exchange", "SharePoint", "Teams");

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(anyString())).thenReturn(partitionsCreatedCounter);
        crawler = new DimensionalTimeSliceCrawler(client, pluginMetrics);
        crawler.initialize(LOG_TYPES);
    }

    @Test
    void testCrawl_withIncrementalSync() {
        Instant lastPollTime = Instant.now().minus(Duration.ofHours(1));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(lastPollTime, 0);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        verify(coordinator).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(LOG_TYPES.size())).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(LOG_TYPES.size(), createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(lastPollTime, workerState.getStartTime());
            assertEquals(latest, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void testCrawl_withHistoricalSync() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.HOURS);
        int lookbackHours = 3;
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, lookbackHours);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        // Expecting (lookbackHours + 1) * LOG_TYPES.size() partitions
        int expectedPartitions = (lookbackHours + 1) * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        // Verify first hour's partitions
        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(now.minus(Duration.ofHours(lookbackHours)), workerState.getStartTime());
            assertEquals(now.minus(Duration.ofHours(lookbackHours-1)), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void testCreateWorkerPartition() {
        Instant start = Instant.parse("2024-10-30T00:00:00Z");
        Instant end = start.plus(Duration.ofHours(1));
        String logType = "Exchange";

        crawler.createWorkerPartition(start, end, logType, coordinator);

        verify(coordinator).createPartition(partitionCaptor.capture());
        verify(partitionsCreatedCounter).increment();

        SaasSourcePartition partition = partitionCaptor.getValue();
        DimensionalTimeSliceWorkerProgressState state =
                (DimensionalTimeSliceWorkerProgressState) partition.getProgressState().get();
        assertEquals(start, state.getStartTime());
        assertEquals(end, state.getEndTime());
        assertEquals(logType, state.getDimensionType());
    }

    @Test
    void testExecutePartition() {
        DimensionalTimeSliceWorkerProgressState state = new DimensionalTimeSliceWorkerProgressState();
        Buffer<Record<Event>> buffer = mock(Buffer.class);
        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);

        crawler.executePartition(state, buffer, ackSet);

        verify(client).executePartition(eq(state), eq(buffer), eq(ackSet));
    }

    @Test
    void testInitialize_ThrowsExceptionWhenCalledTwice() {
        assertThrows(IllegalStateException.class, () -> {
            crawler.initialize(LOG_TYPES); // Second call should throw
        });
    }

    @Test
    void testInitialize_ThrowsExceptionWithNullLogTypes() {
        DimensionalTimeSliceCrawler newCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics);
        assertThrows(NullPointerException.class, () -> {
            newCrawler.initialize(null);
        });
    }
}