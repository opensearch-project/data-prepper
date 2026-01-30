/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.ConfigurableIntervalLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

/**
 * An enhanced dimensional time slice crawler that supports configurable partition creation
 * intervals per dimension type. This allows different event types to have different
 * partition creation frequencies (e.g., some every 5 minutes, others daily).
 * 
 * Extends DimensionalTimeSliceCrawler to maintain backward compatibility while adding
 * the ability to configure partition schedules per dimension type.
 */
@Named
public class ConfigurableIntervalDimensionalTimeSliceCrawler extends DimensionalTimeSliceCrawler {
    private static final Logger log = LoggerFactory.getLogger(ConfigurableIntervalDimensionalTimeSliceCrawler.class);
    private static final String CONFIGURABLE_INTERVAL_PARTITIONS_CREATED = "configurableIntervalPartitionsCreated";
    private static final String CONFIGURABLE_INTERVAL_PARTITIONS_SKIPPED = "configurableIntervalPartitionsSkipped";
    private static final String LAST_UPDATED_KEY = "last_updated|";

    private final Counter configurablePartitionsCreatedCounter;
    private final Counter configurablePartitionsSkippedCounter;
    private final AtomicLong partitionCreationCount = new AtomicLong(0);
    private Function<String, Duration> partitionScheduleProvider;
    private List<String> dimensionTypes;

    public ConfigurableIntervalDimensionalTimeSliceCrawler(CrawlerClient client,
                                                          PluginMetrics pluginMetrics) {
        super(client, pluginMetrics);
        this.configurablePartitionsCreatedCounter = pluginMetrics.counter(CONFIGURABLE_INTERVAL_PARTITIONS_CREATED);
        this.configurablePartitionsSkippedCounter = pluginMetrics.counter(CONFIGURABLE_INTERVAL_PARTITIONS_SKIPPED);
    }

    /**
     * Sets the partition schedule provider function.
     * 
     * @param partitionScheduleProvider Function that returns the Duration for partition
     *                                creation interval given a dimension type
     */
    public void setPartitionScheduleProvider(Function<String, Duration> partitionScheduleProvider) {
        this.partitionScheduleProvider = partitionScheduleProvider;
    }

    /**
     * Sets the dimension types for this crawler.
     * 
     * @param dimensionTypes List of dimension types to process
     */
    public void setDimensionTypes(List<String> dimensionTypes) {
        this.dimensionTypes = dimensionTypes;
    }

    /**
     * Gets the dimension types for this crawler.
     * 
     * @return List of dimension types
     */
    protected List<String> getDimensionTypes() {
        return this.dimensionTypes;
    }

    /**
     * Creates partitions using configurable intervals per dimension type.
     * Only creates partitions for dimension types whose schedule interval has elapsed.
     */
    @Override
    public Instant crawl(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator) {
        if (partitionScheduleProvider == null) {
            log.warn("No partition schedule provider configured, falling back to default behavior");
            return super.crawl(leaderPartition, coordinator);
        }

        long startCount = partitionCreationCount.get();
        Instant latestModifiedTime = createPartitionsWithConfigurableIntervals(leaderPartition, coordinator);
        long partitionsInThisCrawl = partitionCreationCount.get() - startCount;
        
        log.info("Total configurable interval partitions created in this crawl: {}", partitionsInThisCrawl);
        return latestModifiedTime;
    }

    private Instant createPartitionsWithConfigurableIntervals(LeaderPartition leaderPartition,
                                                             EnhancedSourceCoordinator coordinator) {
        ConfigurableIntervalLeaderProgressState leaderProgressState =
                (ConfigurableIntervalLeaderProgressState) leaderPartition.getProgressState().get();

        if (leaderProgressState.getRemainingHours() == 0) {
            return createPartitionsForIncrementalSyncWithSchedules(leaderPartition, coordinator);
        } else {
            return createPartitionsForHistoricalPullWithSchedules(leaderPartition, coordinator);
        }
    }

    /**
     * Creates partitions for historical data pull with schedule awareness.
     * Falls back to parent behavior as historical pulls typically need all data.
     */
    private Instant createPartitionsForHistoricalPullWithSchedules(LeaderPartition leaderPartition,
                                                                  EnhancedSourceCoordinator coordinator) {
        log.info("Historical pull mode - creating partitions for all dimension types");
        
        // For historical pulls, we need all data regardless of schedules
        // For historical pulls, convert the configurable state to standard state
        ConfigurableIntervalLeaderProgressState configurableState = 
            (ConfigurableIntervalLeaderProgressState) leaderPartition.getProgressState().get();
        
        DimensionalTimeSliceLeaderProgressState standardState = 
            new DimensionalTimeSliceLeaderProgressState(
                configurableState.getLastPollTime(),
                configurableState.getRemainingHours());
        
        LeaderPartition standardLeaderPartition = new LeaderPartition(standardState);
        
        // Call parent's crawl method which will handle historical pulls
        return super.crawl(standardLeaderPartition, coordinator);
    }

    /**
     * Creates partitions for incremental sync with per-dimension-type scheduling.
     * Only creates partitions for dimension types whose schedule interval has elapsed.
     */
    private Instant createPartitionsForIncrementalSyncWithSchedules(LeaderPartition leaderPartition,
                                                                   EnhancedSourceCoordinator coordinator) {
        Instant currentTime = Instant.now().minusSeconds(WAIT_SECONDS_BEFORE_PARTITION_CREATION);
        ConfigurableIntervalLeaderProgressState leaderProgressState =
                (ConfigurableIntervalLeaderProgressState) leaderPartition.getProgressState().get();
        
        Instant globalLastPollTime = leaderProgressState.getLastPollTime();
        Map<String, Instant> lastPartitionTimes = leaderProgressState.getLastPartitionTimePerDimensionType();
        
        // Initialize partition count for this crawl
        long crawlStartCount = partitionCreationCount.get();
        
        // Initialize missing dimension types with original baseline to prevent state corruption
        // This ensures each dimension type maintains its own stable timing, preventing 5-minute events
        // from corrupting the scheduling of 10-minute events by updating the global baseline
        for (String dimensionType : getDimensionTypes()) {
            if (!lastPartitionTimes.containsKey(dimensionType)) {
                lastPartitionTimes.put(dimensionType, globalLastPollTime);
            }
        }
        
        boolean anyPartitionsCreated = false;
        Instant latestPartitionTime = globalLastPollTime;

        for (String dimensionType : getDimensionTypes()) {
            Duration scheduleInterval = partitionScheduleProvider.apply(dimensionType);
            Instant lastPartitionTime = lastPartitionTimes.getOrDefault(dimensionType, globalLastPollTime);
            Instant nextScheduledTime = lastPartitionTime.plus(scheduleInterval);

            if (currentTime.isAfter(nextScheduledTime)) {
                log.info("Creating partition for dimension type '{}' (schedule: {}, last partition: {})",
                        dimensionType, scheduleInterval, lastPartitionTime);
                
                createWorkerPartitionForDimensionType(lastPartitionTime, currentTime, dimensionType, coordinator);
                
                // Update last partition time for this dimension type
                lastPartitionTimes.put(dimensionType, currentTime);
                anyPartitionsCreated = true;
                
                if (currentTime.isAfter(latestPartitionTime)) {
                    latestPartitionTime = currentTime;
                }
                
                // Increment both counters for reliability
                configurablePartitionsCreatedCounter.increment();
                partitionCreationCount.incrementAndGet();
            } else {
                log.debug("Skipping partition creation for dimension type '{}' - next scheduled time: {}",
                         dimensionType, nextScheduledTime);
                configurablePartitionsSkippedCounter.increment();
            }
        }

        if (anyPartitionsCreated) {
            updateConfigurableIntervalLeaderProgressState(leaderPartition, 0, latestPartitionTime, 
                                                         lastPartitionTimes, coordinator);
            return latestPartitionTime;
        }

        return globalLastPollTime;
    }

    /**
     * Creates a worker partition for a specific dimension type.
     */
    private void createWorkerPartitionForDimensionType(Instant startTime, Instant endTime,
                                                      String dimensionType, EnhancedSourceCoordinator coordinator) {
        DimensionalTimeSliceWorkerProgressState workerState = new DimensionalTimeSliceWorkerProgressState();
        workerState.setPartitionCreationTime(Instant.now());
        workerState.setStartTime(startTime);
        workerState.setEndTime(endTime);
        workerState.setDimensionType(dimensionType);

        SaasSourcePartition partition = new SaasSourcePartition(workerState, LAST_UPDATED_KEY + UUID.randomUUID());
        coordinator.createPartition(partition);
    }

    /**
     * Updates the configurable interval leader progress state.
     */
    private void updateConfigurableIntervalLeaderProgressState(LeaderPartition leaderPartition,
                                                              int remainingHours,
                                                              Instant updatedPollTime,
                                                              Map<String, Instant> lastPartitionTimes,
                                                              EnhancedSourceCoordinator coordinator) {
        ConfigurableIntervalLeaderProgressState state =
                (ConfigurableIntervalLeaderProgressState) leaderPartition.getProgressState().get();
        
        state.setRemainingHours(remainingHours);
        state.setLastPollTime(updatedPollTime);
        state.setLastPartitionTimePerDimensionType(lastPartitionTimes);
        
        leaderPartition.setLeaderProgressState(state);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
    }
}
