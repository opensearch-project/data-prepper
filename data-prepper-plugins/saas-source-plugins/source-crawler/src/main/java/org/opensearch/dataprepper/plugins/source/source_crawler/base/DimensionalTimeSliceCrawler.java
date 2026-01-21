package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

/**
 * A crawler implementation that partitions work along two dimensions:
 * 1. Dimension type (e.g., log types)
 * 2. Time slices (configurable time windows)
 * This crawler supports both historical data ingestion and incremental updates,
 * creating separate partitions for each combination of dimension type and time window.
 * Supports minute-level granularity for historical pulls (e.g., PT15M, PT30M).
 */
@Named
public class DimensionalTimeSliceCrawler implements Crawler<DimensionalTimeSliceWorkerProgressState> {
    private static final Logger log = LoggerFactory.getLogger(DimensionalTimeSliceCrawler.class);
    // delay five minutes for partition creation on latest time duration to ensure the newly generated events are queryable
    // In general, newly generated events become queryable after 30 ~ 120 second
    protected static final long WAIT_SECONDS_BEFORE_PARTITION_CREATION = 300;
    private static final String DIMENSIONAL_TIME_SLICE_WORKER_PARTITIONS_CREATED = "dimensionalTimeSliceWorkerPartitionsCreated";
    private static final String WORKER_PARTITION_WAIT_TIME = "workerPartitionWaitTime";
    private static final String WORKER_PARTITION_PROCESS_LATENCY = "workerPartitionProcessLatency";
    private static final Duration HOUR_DURATION = Duration.ofHours(1);
    private static final long MINUTES_PER_HOUR = 60;
    private static final long WAIT_MINUTES_BEFORE_PARTITION_CREATION = WAIT_SECONDS_BEFORE_PARTITION_CREATION / 60; // 5 minutes

    private final CrawlerClient client;
    private final Counter partitionsCreatedCounter;
    private final Timer partitionWaitTimeTimer;
    private final Timer partitionProcessLatencyTimer;
    private final AtomicLong localPartitionCounter = new AtomicLong(0);
    private List<String> dimensionTypes;
    private static final String LAST_UPDATED_KEY = "last_updated|";

    public DimensionalTimeSliceCrawler(CrawlerClient client,
                                       PluginMetrics pluginMetrics) {
        this.client = client;
        this.partitionsCreatedCounter = pluginMetrics.counter(DIMENSIONAL_TIME_SLICE_WORKER_PARTITIONS_CREATED);
        this.partitionWaitTimeTimer = pluginMetrics.timer(WORKER_PARTITION_WAIT_TIME);
        this.partitionProcessLatencyTimer = pluginMetrics.timer(WORKER_PARTITION_PROCESS_LATENCY);
    }

    /**
     * Initializes the crawler with a list of dimension types.
     * Must be called before the crawler can be used.
     */
    public void initialize(List<String> dimensionTypes) {
        if (this.dimensionTypes != null) {
            throw new IllegalStateException("Crawler already initialized");
        }
        this.dimensionTypes = Objects.requireNonNull(dimensionTypes, "dimensionTypes must not be null");
    }

    /**
     * Creates partitions for the current crawl cycle. For historical pulls, creates time-based partitions
     * for each dimension type. For incremental sync, creates one partition per dimension type.
     */
    @Override
    public Instant crawl(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator) {
        long startCount = localPartitionCounter.get();

        Instant latestModifiedTime = createPartitions(leaderPartition, coordinator);

        long partitionsInThisCrawl = localPartitionCounter.get() - startCount;
        log.info("Total partitions created in this crawl: {}", partitionsInThisCrawl);
        return latestModifiedTime;
    }

    @Override
    public void executePartition(DimensionalTimeSliceWorkerProgressState state, Buffer<Record<Event>> buffer, AcknowledgementSet acknowledgementSet) {
        log.info("Processing partition - DimensionType: {}, TimeRange: {} to {}",
                 state.getDimensionType(), state.getStartTime(), state.getEndTime());
        partitionWaitTimeTimer.record(Duration.between(state.getPartitionCreationTime(), Instant.now()));
        partitionProcessLatencyTimer.record(() -> client.executePartition(state, buffer, acknowledgementSet));
    }

    private Instant createPartitions(LeaderPartition leaderPartition,
                                     EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceLeaderProgressState leaderProgressState =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();

        Instant remainingDuration = leaderProgressState.getRemainingDuration();
        Instant lastPollTime = leaderProgressState.getLastPollTime();
        if (remainingDuration.equals(lastPollTime)) {
            return createPartitionsForIncrementalSync(leaderPartition, coordinator);
        } else {
            return createPartitionsForHistoricalPull(leaderPartition, coordinator);
        }
    }

    /**
     * Creates partitions for historical data pull. Creates time-based partitions
     * for each dimension type, working backwards from the current time.
     * Supports both sub-hour (minute-based) and hour-based time ranges.
     */
    private Instant createPartitionsForHistoricalPull(LeaderPartition leaderPartition,
                                                      EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceLeaderProgressState leaderProgressState =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();
        Instant initialTime = leaderProgressState.getLastPollTime();
        Instant latestModifiedTime = initialTime.minusSeconds(WAIT_SECONDS_BEFORE_PARTITION_CREATION);
        Instant remainingDuration = leaderProgressState.getRemainingDuration();
        long remainingMinutes = Duration.between(remainingDuration, initialTime).toMinutes();

        // For sub-hour time ranges (less than 60 minutes), create a single partition
        if (remainingMinutes < MINUTES_PER_HOUR) {
            log.info("Creating partition for sub-hour historical pull: {} minutes", remainingMinutes);
            Instant startTime = initialTime.minus(Duration.ofMinutes(remainingMinutes));
            Instant endTime;
            if (remainingMinutes <= WAIT_MINUTES_BEFORE_PARTITION_CREATION) {
                // For very small ranges, skip the 5-minute delay to create a valid partition
                endTime = initialTime;
            } else {
                endTime = latestModifiedTime;
            }

            createWorkerPartitionsForDimensionTypes(startTime, endTime, coordinator);
            updateLeaderProgressState(leaderPartition, endTime, coordinator);
            return endTime;
        }

        // For hour or longer time ranges, use hourly partitioning
        long remainingHours = remainingMinutes / MINUTES_PER_HOUR;
        long extraMinutes = remainingMinutes % MINUTES_PER_HOUR;
        Instant latestHour = initialTime.truncatedTo(ChronoUnit.HOURS);

        // Create hourly partitions for complete hours
        for (long i = remainingHours; i > 1; i--) {
            Instant startTime = latestHour.minus(Duration.ofHours(i));
            // For the first partition, include any extra minutes
            if (i == remainingHours && extraMinutes > 0) {
                startTime = startTime.minus(Duration.ofMinutes(extraMinutes));
            }
            Instant endTime = latestHour.minus(Duration.ofHours(i - 1));

            createWorkerPartitionsForDimensionTypes(startTime, endTime, coordinator);
        }

        if (latestModifiedTime.isAfter(latestHour)) {
            // if checkpointing time is after the latest hour, create one partition for last hour
            // and one from latest hour to checkpointing time
            createWorkerPartitionsForDimensionTypes(latestHour.minus(Duration.ofHours(1)), latestHour, coordinator);
            createWorkerPartitionsForDimensionTypes(latestHour, latestModifiedTime, coordinator);
        } else {
            // if checkpointing time is not later than the latest hour, create one partition from 1 hour ago to checkpointing time
            createWorkerPartitionsForDimensionTypes(latestHour.minus(Duration.ofHours(1)), latestModifiedTime, coordinator);
        }

        updateLeaderProgressState(leaderPartition, latestModifiedTime, coordinator);

        return latestModifiedTime;
    }

    /**
     * Creates partitions for incremental sync. Creates one partition per dimension type
     * from the last poll time to current time.
     */
    private Instant createPartitionsForIncrementalSync(LeaderPartition leaderPartition,
                                                    EnhancedSourceCoordinator coordinator) {
        Instant latestModifiedTime = Instant.now().minusSeconds(WAIT_SECONDS_BEFORE_PARTITION_CREATION);
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        Instant lastPollTime = leaderProgressState.getLastPollTime();

        if (lastPollTime.isBefore(latestModifiedTime)) {
            // Create one partition from lastPollTime to latestModifiedTime for each type
            createWorkerPartitionsForDimensionTypes(lastPollTime, latestModifiedTime, coordinator);

            updateLeaderProgressState(leaderPartition, latestModifiedTime, coordinator);
            return latestModifiedTime;
        }

        return lastPollTime;
    }

    void createWorkerPartitionsForDimensionTypes(Instant startTime, Instant endTime, EnhancedSourceCoordinator coordinator) {
        for (String dimensionType : dimensionTypes) {
            DimensionalTimeSliceWorkerProgressState workerState = new DimensionalTimeSliceWorkerProgressState();
            workerState.setPartitionCreationTime(Instant.now());
            workerState.setStartTime(startTime);
            workerState.setEndTime(endTime);
            workerState.setDimensionType(dimensionType);

            SaasSourcePartition partition = new SaasSourcePartition(workerState, LAST_UPDATED_KEY + UUID.randomUUID());
            coordinator.createPartition(partition);
            
            // Increment both counters for reliability
            partitionsCreatedCounter.increment();
            localPartitionCounter.incrementAndGet();
        }
    }

    /**
     * Updates the leader progress state with the latest poll timestamp and remaining duration.
     * This method also persists the updated state in the source coordinator.
     */
    private void updateLeaderProgressState(LeaderPartition leaderPartition,
                                           Instant updatedPollTime,
                                           EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceLeaderProgressState state =
                (DimensionalTimeSliceLeaderProgressState) leaderPartition.getProgressState().get();
        state.setRemainingDuration(updatedPollTime);
        state.setLastPollTime(updatedPollTime);
        leaderPartition.setLeaderProgressState(state);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
    }
}
