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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TimeSliceCrawlerTest {

    @Mock
    private CrawlerClient client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter partitionsCreatedCounter;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @InjectMocks
    private TimeSliceCrawler crawler;

    @Captor
    private ArgumentCaptor<SaasSourcePartition> partitionCaptor;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(anyString())).thenReturn(partitionsCreatedCounter);

        crawler = new TimeSliceCrawler(client, pluginMetrics);
    }

    @Test
    void testCrawl_withIncrementalSync() {
        CrowdStrikeLeaderProgressState state = new CrowdStrikeLeaderProgressState(Instant.now().minus(Duration.ofHours(1)), 0);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        verify(coordinator, times(1)).createPartition(any(SaasSourcePartition.class));
        verify(coordinator, times(1)).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(1)).increment();
    }

    @Test
    void testCrawl_withHistoricalSync() {
        Instant now = Instant.now().truncatedTo(ChronoUnit.DAYS);
        CrowdStrikeLeaderProgressState state = new CrowdStrikeLeaderProgressState(now, 2); // simulate 2 days of history
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        // Expecting 3 partitions: 2 for history + 1 for today
        verify(coordinator, times(3)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(3)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(3, createdPartitions.size());

        // Ensure first partition is from 2 days ago
        Instant expectedStart = now.minus(Duration.ofDays(2));
        CrowdStrikeWorkerProgressState workerProgressState = (CrowdStrikeWorkerProgressState) createdPartitions.get(0).getProgressState().get();
        assertEquals(expectedStart, workerProgressState.getStartTime());
        assertEquals(expectedStart.plus(Duration.ofDays(1)), workerProgressState.getEndTime());
    }

    @Test
    void testPartitionsCreatedCounterIncremented() {
        Instant start = Instant.parse("2024-10-30T00:00:00Z");
        Instant end = start.plus(Duration.ofDays(1));

        crawler.createWorkerPartition(start, end, coordinator);

        verify(partitionsCreatedCounter, times(1)).increment();
    }

    @Test
    void testExecutePartition() {
        SaasWorkerProgressState state = new CrowdStrikeWorkerProgressState();
        Buffer<Record<Event>> buffer = mock(Buffer.class);
        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);

        crawler.executePartition(state, buffer, ackSet);

        verify(client, times(1)).executePartition(eq(state), eq(buffer), eq(ackSet));
    }
}
