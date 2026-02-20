package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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

import static org.opensearch.dataprepper.plugins.source.source_crawler.base.DimensionalTimeSliceCrawler.WAIT_BEFORE_PARTITION_CREATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class DimensionalTimeSliceCrawlerTest {

    @Mock
    private CrawlerClient client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter partitionsCreatedCounter;

    @Mock
    private Timer partitionWaitTimeTimer;

    @Mock
    private Timer partitionProcessLatencyTimer;

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
        when(pluginMetrics.timer("workerPartitionWaitTime")).thenReturn(partitionWaitTimeTimer);
        when(pluginMetrics.timer("workerPartitionProcessLatency")).thenReturn(partitionProcessLatencyTimer);
        crawler = new DimensionalTimeSliceCrawler(client, pluginMetrics);
        crawler.initialize(LOG_TYPES);
    }

    @Test
    void crawl_withIncrementalSync_whenLastPollBeforeWaitWindow_shouldCreateOnePartitionPerLogType() {
        Instant lastPollTime = Instant.now().minusSeconds(400);
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(lastPollTime, Instant.now());
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
    void crawl_withIncrementalSync_whenLastPollWithinWaitWindow_shouldNotCreatePartitions() {
        Instant lastPollTime = Instant.now().minusSeconds(10);
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(lastPollTime, Instant.now());
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        verify(coordinator, never()).createPartition(partitionCaptor.capture());
        verify(coordinator, never()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, never()).increment();
    }

    @Test
    void crawl_withHistoricalSync_whenInitialTimeInFirst5MinutesOfHour_shouldCreateTwoPartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION).minusSeconds(1);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(120));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        // Expecting (lookbackMinutes/60 + 1) * LOG_TYPES.size() partitions
        int expectedPartitions = 2 * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        // Verify first hour's partitions
        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(2)), workerState.getStartTime());
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }

        // Verify previous hour's partitions
        for (int i = LOG_TYPES.size(); i < LOG_TYPES.size() * 2; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size()), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withHistoricalSync_whenInitialTimeAfterFirst5MinutesOfHour_shouldCreateThreePartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION).plusSeconds(1);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(120));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        // Expecting (lookbackMinutes/60 + 1) * LOG_TYPES.size() partitions
        int expectedPartitions = 3 * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        // Verify first hour's partitions
        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(2)), workerState.getStartTime());
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }

        // Verify previous hour's partitions
        for (int i = LOG_TYPES.size(); i < LOG_TYPES.size() * 2; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getStartTime());
            assertEquals(latestHour, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size()), workerState.getDimensionType());
        }

        // Verify latest hour's partitions
        for (int i = LOG_TYPES.size() * 2; i < LOG_TYPES.size() * 3; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour, workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size() * 2), workerState.getDimensionType());
        }
    }

    @Test
    void createWorkerPartitionsForDimensionTypes_shouldCreateOnePartitionPerLogTypeForTimeRange() {
        Instant start = Instant.parse("2024-10-30T00:00:00Z");
        Instant end = start.plus(Duration.ofHours(1));

        crawler.createWorkerPartitionsForDimensionTypes(start, end, coordinator);

        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        // Verify first hour's partitions
        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(start, workerState.getStartTime());
            assertEquals(end, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void executePartition_shouldDelegateToClientAndRecordMetrics() {
        DimensionalTimeSliceWorkerProgressState state = new DimensionalTimeSliceWorkerProgressState();
        state.setPartitionCreationTime(Instant.now().minusSeconds(1));
        Buffer<Record<Event>> buffer = mock(Buffer.class);
        AcknowledgementSet ackSet = mock(AcknowledgementSet.class);

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(partitionProcessLatencyTimer).record(any(Runnable.class));
        doNothing().when(partitionWaitTimeTimer).record(any(Duration.class));

        crawler.executePartition(state, buffer, ackSet);

        verify(client).executePartition(eq(state), eq(buffer), eq(ackSet));
        verify(partitionProcessLatencyTimer).record(any(Runnable.class));
        verify(partitionWaitTimeTimer).record(any(Duration.class));
    }

    @Test
    void initialize_whenCalledTwice_shouldThrowIllegalStateException() {
        assertThrows(IllegalStateException.class, () -> {
            crawler.initialize(LOG_TYPES); // Second call should throw
        });
    }

    @Test
    void initialize_whenLogTypesNull_shouldThrowNullPointerException() {
        DimensionalTimeSliceCrawler newCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics);
        assertThrows(NullPointerException.class, () -> {
            newCrawler.initialize(null);
        });
    }
    @Test
    void crawl_withSubHourHistoricalSync_with15MinuteLookback_shouldCreateOnePartitionPerLogType() {
        Instant initialTime = Instant.now();
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(15));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);

        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(15)), workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withSubHourHistoricalSync_with30MinuteLookback_shouldCreateOnePartitionPerLogType() {
        Instant initialTime = Instant.now();
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(30));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);

        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(30)), workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withSubHourHistoricalSync_with45MinuteLookback_shouldCreateOnePartitionPerLogType() {
        Instant initialTime = Instant.now();
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(45));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);

        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(45)), workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withIncrementalSync_whenLastPollExactly5MinutesAgo_shouldNotCreatePartitions() {
        Instant lastPollTime = Instant.now().minus(WAIT_BEFORE_PARTITION_CREATION).plusSeconds(1);
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(lastPollTime, lastPollTime.plus(Duration.ofMinutes(4)));
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);

        verify(coordinator, never()).createPartition(partitionCaptor.capture());
        verify(coordinator, never()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, never()).increment();
        assertEquals(lastPollTime, latest);
    }

    @Test
    void crawl_withHistoricalSync_with1HourLookback_shouldCreateTwoPartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION).plusSeconds(1);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(60));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);

        final int expectedHourlyPartitionsPerLogType = 2;
        int expectedPartitions = expectedHourlyPartitionsPerLogType * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getStartTime());
            assertEquals(latestHour, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }

        for (int i = LOG_TYPES.size(); i < LOG_TYPES.size() * expectedHourlyPartitionsPerLogType; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour, workerState.getStartTime());
            assertEquals(initialTime.minus(WAIT_BEFORE_PARTITION_CREATION), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size()), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withHistoricalSync_with3HourLookback_shouldCreateFourPartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION).plusSeconds(1);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(180));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        final int expectedHourlyPartitionsPerLogType = 4;
        int expectedPartitions = expectedHourlyPartitionsPerLogType * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());
    }

    @Test
    void crawl_withHistoricalSync_with2Hours5MinutesLookback_shouldCreateThreePartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION).plusSeconds(1);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(125));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        int expectedPartitions = 3 * LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        DimensionalTimeSliceWorkerProgressState firstWorkerState =
                (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(0).getProgressState().get();
        assertEquals(latestHour.minus(Duration.ofHours(2)).minus(Duration.ofMinutes(5)), firstWorkerState.getStartTime());
        assertEquals(latestHour.minus(Duration.ofHours(1)), firstWorkerState.getEndTime());
    }

    @Test
    void crawl_withSubHourHistoricalSync_withVerySmallRange_shouldCreateOnePartitionWithEndTimeAtInitialTime() {
        Instant initialTime = Instant.now();
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(3));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(3)), workerState.getStartTime());
            assertEquals(initialTime, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withSubHourHistoricalSync_withExactly5MinuteLookback_shouldCreateOnePartitionWithEndTimeAtInitialTime() {
        Instant initialTime = Instant.now();
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(5));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(5)), workerState.getStartTime());
            assertEquals(initialTime, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withHistoricalSync_whenLatestModifiedTimeEqualsLatestHour_shouldCreateOnePartitionPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(WAIT_BEFORE_PARTITION_CREATION);
        Instant lookbackDuration = initialTime.minus(Duration.ofMinutes(60));
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(initialTime, lookbackDuration);
        LeaderPartition leaderPartition = new LeaderPartition(state);

        Instant latest = crawler.crawl(leaderPartition, coordinator);

        assertNotNull(latest);
        int expectedPartitions = LOG_TYPES.size();
        verify(coordinator, times(expectedPartitions)).createPartition(partitionCaptor.capture());
        verify(coordinator, atLeastOnce()).saveProgressStateForPartition(eq(leaderPartition), any());
        verify(partitionsCreatedCounter, times(expectedPartitions)).increment();

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        assertEquals(expectedPartitions, createdPartitions.size());

        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getStartTime());
            assertEquals(latestHour, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void createWorkerPartitionsForDimensionTypes_shouldSetPartitionCreationTimeWithinNow() {
        Instant start = Instant.parse("2024-10-30T00:00:00Z");
        Instant end = start.plus(Duration.ofHours(1));
        Instant beforeCreation = Instant.now();

        crawler.createWorkerPartitionsForDimensionTypes(start, end, coordinator);

        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();

        Instant afterCreation = Instant.now();

        for (SaasSourcePartition partition : createdPartitions) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) partition.getProgressState().get();
            assertNotNull(workerState.getPartitionCreationTime());

            assertTrue(workerState.getPartitionCreationTime().isAfter(beforeCreation.minusSeconds(1)));
            assertTrue(workerState.getPartitionCreationTime().isBefore(afterCreation.plusSeconds(1)));
        }
    }


    @Test
    void initialize_withEmptyList_shouldSucceedButSecondCallShouldThrow() {
        DimensionalTimeSliceCrawler newCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics);
        List<String> emptyList = Arrays.asList();
        newCrawler.initialize(emptyList);
        assertThrows(IllegalStateException.class, () -> {
            newCrawler.initialize(emptyList);
        });
    }
}