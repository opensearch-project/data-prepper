package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig.DEFAULT_PARTITION_CREATION_WAIT;
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

    @Mock
    private CrawlerSourceConfig sourceConfig;

    private DimensionalTimeSliceCrawler crawler;

    @Captor
    private ArgumentCaptor<SaasSourcePartition> partitionCaptor;

    private static final List<String> LOG_TYPES = Arrays.asList("Exchange", "SharePoint", "Teams");
    private static final Duration CUSTOM_WAIT = Duration.ofMinutes(15);

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
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT).minusSeconds(1);
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size()), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withHistoricalSync_whenInitialTimeAfterFirst5MinutesOfHour_shouldCreateThreePartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT).plusSeconds(1);
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withIncrementalSync_whenLastPollJustInsideDefaultWait_shouldNotCreatePartitions() {
        Instant lastPollTime = Instant.now().minus(DEFAULT_PARTITION_CREATION_WAIT).plusSeconds(1);
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
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT).plusSeconds(1);
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
            assertEquals(initialTime.minus(DEFAULT_PARTITION_CREATION_WAIT), workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i - LOG_TYPES.size()), workerState.getDimensionType());
        }
    }

    @Test
    void crawl_withHistoricalSync_with3HourLookback_shouldCreateFourPartitionsPerLogType() {
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT).plusSeconds(1);
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
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT).plusSeconds(1);
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
        Instant initialTime = latestHour.plus(DEFAULT_PARTITION_CREATION_WAIT);
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

    @Test
    void constructor_withoutSourceConfig_shouldUseDefaultPartitionCreationWait() {
        assertEquals(Duration.ofMinutes(5), DEFAULT_PARTITION_CREATION_WAIT);

        assertCrawlerUsesWait(crawler, DEFAULT_PARTITION_CREATION_WAIT);
    }

    @Test
    void constructor_withSourceConfig_shouldUseConfiguredPartitionCreationWait() {
        when(sourceConfig.getPartitionCreationWait()).thenReturn(CUSTOM_WAIT);
        DimensionalTimeSliceCrawler configuredCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics, sourceConfig);
        configuredCrawler.initialize(LOG_TYPES);

        assertCrawlerUsesWait(configuredCrawler, CUSTOM_WAIT);
    }

    @Test
    void constructor_withSourceConfigNotOverridingWait_shouldUseDefaultPartitionCreationWait() {
        DimensionalTimeSliceCrawler configuredCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics, new CrawlerSourceConfig() {
            @Override
            public int getNumberOfWorkers() {
                return 1;
            }

            @Override
            public boolean isAcknowledgments() {
                return false;
            }
        });
        configuredCrawler.initialize(LOG_TYPES);

        assertCrawlerUsesWait(configuredCrawler, DEFAULT_PARTITION_CREATION_WAIT);
    }

    @Test
    void constructor_withSourceConfigReturningInvalidWait_shouldThrowIllegalArgumentException() {
        when(sourceConfig.getPartitionCreationWait()).thenReturn(Duration.ofMinutes(-1));
        assertThrows(IllegalArgumentException.class, () -> new DimensionalTimeSliceCrawler(client, pluginMetrics, sourceConfig));
    }

    @Test
    void constructor_withNullWait_shouldThrowNullPointerException() {
        assertThrows(NullPointerException.class, () -> new DimensionalTimeSliceCrawler(client, pluginMetrics, (Duration) null));
    }

    @Test
    void constructor_withNegativeWait_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new DimensionalTimeSliceCrawler(client, pluginMetrics, Duration.ofSeconds(-1)));
        assertTrue(exception.getMessage().contains("must not be negative"));
    }

    @Test
    void constructor_withWaitAboveMaximum_shouldThrowIllegalArgumentException() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new DimensionalTimeSliceCrawler(client, pluginMetrics, DimensionalTimeSliceCrawler.MAX_PARTITION_CREATION_WAIT.plusSeconds(1)));
        assertTrue(exception.getMessage().contains("must not exceed"));
    }

    @Test
    void constructor_withZeroAndMaximumWait_shouldSucceed() {
        assertNotNull(new DimensionalTimeSliceCrawler(client, pluginMetrics, Duration.ZERO));
        assertNotNull(new DimensionalTimeSliceCrawler(client, pluginMetrics, DimensionalTimeSliceCrawler.MAX_PARTITION_CREATION_WAIT));
    }

    @Test
    void springContext_shouldCreateCrawlerWithPartitionCreationWaitFromSourceConfigBean() {
        when(sourceConfig.getPartitionCreationWait()).thenReturn(CUSTOM_WAIT);
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            context.getBeanFactory().registerSingleton("crawlerClient", client);
            context.getBeanFactory().registerSingleton("pluginMetrics", pluginMetrics);
            context.getBeanFactory().registerSingleton("sourceConfig", sourceConfig);
            context.register(DimensionalTimeSliceCrawler.class);
            context.refresh();

            DimensionalTimeSliceCrawler injectedCrawler = context.getBean(DimensionalTimeSliceCrawler.class);
            injectedCrawler.initialize(LOG_TYPES);

            assertCrawlerUsesWait(injectedCrawler, CUSTOM_WAIT);
        }
    }

    @Test
    void crawl_withIncrementalSync_withCustomWait_whenLastPollBeforeWaitWindow_shouldCreatePartitionEndingAtNowMinusWait() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant lastPollTime = Instant.now().minus(CUSTOM_WAIT).minusSeconds(1);
        LeaderPartition leaderPartition = new LeaderPartition(new DimensionalTimeSliceLeaderProgressState(lastPollTime, lastPollTime));
        Instant beforeCrawl = Instant.now();

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        Instant afterCrawl = Instant.now();
        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        verify(coordinator).saveProgressStateForPartition(eq(leaderPartition), any());
        assertEndTimeIsNowMinusWait(latest, beforeCrawl, afterCrawl, CUSTOM_WAIT);

        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        for (int i = 0; i < LOG_TYPES.size(); i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(lastPollTime, workerState.getStartTime());
            assertEquals(latest, workerState.getEndTime());
            assertEquals(LOG_TYPES.get(i), workerState.getDimensionType());
        }
        assertEquals(latest, leaderPartition.getProgressState().get().getLastPollTime());
    }

    @Test
    void crawl_withIncrementalSync_withCustomWait_whenLastPollOutsideDefaultButInsideCustomWait_shouldNotCreatePartitions() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant lastPollTime = Instant.now().minus(Duration.ofMinutes(10));
        LeaderPartition leaderPartition = new LeaderPartition(new DimensionalTimeSliceLeaderProgressState(lastPollTime, lastPollTime));

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        assertEquals(lastPollTime, latest);
        verify(coordinator, never()).createPartition(any());
        verify(coordinator, never()).saveProgressStateForPartition(any(), any());
        verify(partitionsCreatedCounter, never()).increment();
    }

    @Test
    void crawl_withIncrementalSync_whenWaitIncreasedFrom5To15Minutes_shouldResumeAtCheckpointWithoutGapOrOverlap() {
        // The crawler only depends on the age of the checkpoint, so time passing is modeled with
        // checkpoints of increasing age relative to Instant.now().
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);

        Instant checkpointJustWrittenUnderOldWait = Instant.now().minus(DEFAULT_PARTITION_CREATION_WAIT);
        LeaderPartition justWritten = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(checkpointJustWrittenUnderOldWait, checkpointJustWrittenUnderOldWait));
        assertEquals(checkpointJustWrittenUnderOldWait, customCrawler.crawl(justWritten, coordinator));

        Instant checkpointJustInsideNewWait = Instant.now().minus(CUSTOM_WAIT).plusSeconds(1);
        LeaderPartition justInside = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(checkpointJustInsideNewWait, checkpointJustInsideNewWait));
        assertEquals(checkpointJustInsideNewWait, customCrawler.crawl(justInside, coordinator));

        verify(coordinator, never()).createPartition(any());
        verify(coordinator, never()).saveProgressStateForPartition(any(), any());

        Instant checkpointPastNewWait = Instant.now().minus(CUSTOM_WAIT).minusSeconds(1);
        LeaderPartition pastWait = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(checkpointPastNewWait, checkpointPastNewWait));
        Instant beforeCrawl = Instant.now();

        Instant latest = customCrawler.crawl(pastWait, coordinator);

        Instant afterCrawl = Instant.now();
        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        for (SaasSourcePartition partition : partitionCaptor.getAllValues()) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) partition.getProgressState().get();
            assertEquals(checkpointPastNewWait, workerState.getStartTime());
            assertEquals(latest, workerState.getEndTime());
        }
        assertEndTimeIsNowMinusWait(latest, beforeCrawl, afterCrawl, CUSTOM_WAIT);
        assertEquals(latest, pastWait.getProgressState().get().getLastPollTime());
    }

    @Test
    void crawl_withSubHourHistoricalSync_withCustomWait_shouldEndPartitionAtInitialTimeMinusWait() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant initialTime = Instant.now();
        LeaderPartition leaderPartition = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(initialTime, initialTime.minus(Duration.ofMinutes(45))));

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        assertEquals(initialTime.minus(CUSTOM_WAIT), latest);
        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        for (SaasSourcePartition partition : partitionCaptor.getAllValues()) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) partition.getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(45)), workerState.getStartTime());
            assertEquals(initialTime.minus(CUSTOM_WAIT), workerState.getEndTime());
        }
        assertEquals(latest, leaderPartition.getProgressState().get().getLastPollTime());
    }

    @Test
    void crawl_withSubHourHistoricalSync_withCustomWait_whenRangeNotLongerThanWait_shouldEndPartitionAtInitialTime() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant initialTime = Instant.now();
        LeaderPartition leaderPartition = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(initialTime, initialTime.minus(Duration.ofMinutes(10))));

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        assertEquals(initialTime, latest);
        verify(coordinator, times(LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        for (SaasSourcePartition partition : partitionCaptor.getAllValues()) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) partition.getProgressState().get();
            assertEquals(initialTime.minus(Duration.ofMinutes(10)), workerState.getStartTime());
            assertEquals(initialTime, workerState.getEndTime());
        }
    }

    @Test
    void crawl_withHistoricalSync_withCustomWait_whenInitialTimeAfterWaitPastHour_shouldCreateThreePartitionsPerLogType() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(CUSTOM_WAIT).plusSeconds(1);
        LeaderPartition leaderPartition = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(initialTime, initialTime.minus(Duration.ofMinutes(120))));

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        assertEquals(initialTime.minus(CUSTOM_WAIT), latest);
        verify(coordinator, times(3 * LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        for (int i = LOG_TYPES.size() * 2; i < LOG_TYPES.size() * 3; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour, workerState.getStartTime());
            assertEquals(initialTime.minus(CUSTOM_WAIT), workerState.getEndTime());
        }
        assertEquals(latest, leaderPartition.getProgressState().get().getLastPollTime());
    }

    @Test
    void crawl_withHistoricalSync_withCustomWait_whenInitialTimeWithinWaitPastHour_shouldCreateTwoPartitionsPerLogType() {
        DimensionalTimeSliceCrawler customCrawler = createCrawlerWithWait(CUSTOM_WAIT);
        Instant latestHour = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant initialTime = latestHour.plus(Duration.ofMinutes(10));
        LeaderPartition leaderPartition = new LeaderPartition(
                new DimensionalTimeSliceLeaderProgressState(initialTime, initialTime.minus(Duration.ofMinutes(120))));

        Instant latest = customCrawler.crawl(leaderPartition, coordinator);

        assertEquals(initialTime.minus(CUSTOM_WAIT), latest);
        verify(coordinator, times(2 * LOG_TYPES.size())).createPartition(partitionCaptor.capture());
        List<SaasSourcePartition> createdPartitions = partitionCaptor.getAllValues();
        for (int i = LOG_TYPES.size(); i < LOG_TYPES.size() * 2; i++) {
            DimensionalTimeSliceWorkerProgressState workerState =
                    (DimensionalTimeSliceWorkerProgressState) createdPartitions.get(i).getProgressState().get();
            assertEquals(latestHour.minus(Duration.ofHours(1)), workerState.getStartTime());
            assertEquals(initialTime.minus(CUSTOM_WAIT), workerState.getEndTime());
        }
    }

    private DimensionalTimeSliceCrawler createCrawlerWithWait(Duration partitionCreationWait) {
        DimensionalTimeSliceCrawler newCrawler = new DimensionalTimeSliceCrawler(client, pluginMetrics, partitionCreationWait);
        newCrawler.initialize(LOG_TYPES);
        return newCrawler;
    }

    private void assertCrawlerUsesWait(DimensionalTimeSliceCrawler crawlerUnderTest, Duration expectedWait) {
        EnhancedSourceCoordinator insideWaitCoordinator = mock(EnhancedSourceCoordinator.class);
        Instant insideWait = Instant.now().minus(expectedWait).plusSeconds(1);
        LeaderPartition insideWaitPartition = new LeaderPartition(new DimensionalTimeSliceLeaderProgressState(insideWait, insideWait));
        assertEquals(insideWait, crawlerUnderTest.crawl(insideWaitPartition, insideWaitCoordinator));
        verify(insideWaitCoordinator, never()).createPartition(any());

        EnhancedSourceCoordinator pastWaitCoordinator = mock(EnhancedSourceCoordinator.class);
        Instant pastWait = Instant.now().minus(expectedWait).minusSeconds(1);
        LeaderPartition pastWaitPartition = new LeaderPartition(new DimensionalTimeSliceLeaderProgressState(pastWait, pastWait));
        Instant beforeCrawl = Instant.now();
        Instant latest = crawlerUnderTest.crawl(pastWaitPartition, pastWaitCoordinator);
        verify(pastWaitCoordinator, times(LOG_TYPES.size())).createPartition(any(SaasSourcePartition.class));
        assertEndTimeIsNowMinusWait(latest, beforeCrawl, Instant.now(), expectedWait);
    }

    private static void assertEndTimeIsNowMinusWait(Instant endTime, Instant beforeCrawl, Instant afterCrawl, Duration wait) {
        assertTrue(!endTime.isBefore(beforeCrawl.minus(wait)), "end time " + endTime + " is before now - " + wait);
        assertTrue(!endTime.isAfter(afterCrawl.minus(wait)), "end time " + endTime + " is after now - " + wait);
    }
}
