/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamCheckpointManager {
    private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointManager.class);
    static final int REGULAR_CHECKPOINT_INTERVAL_MILLIS = 60_000;
    static final int CHANGE_EVENT_COUNT_PER_CHECKPOINT_BATCH = 1000;
    static final String POSITIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME = "positiveAcknowledgementSets";
    static final String NEGATIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME = "negativeAcknowledgementSets";
    static final String CHECKPOINT_COUNT = "checkpointCount";
    static final String NO_DATA_EXTEND_LEASE_COUNT = "noDataExtendLeaseCount";
    static final String GIVE_UP_PARTITION_COUNT = "giveupPartitionCount";

    private final ConcurrentLinkedQueue<ChangeEventStatus> changeEventStatuses = new ConcurrentLinkedQueue<>();
    private final StreamCheckpointer streamCheckpointer;
    private final ExecutorService executorService;
    private final Runnable stopStreamRunnable;
    private final boolean isAcknowledgmentEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Duration acknowledgmentTimeout;
    private final EngineType engineType;
    private final PluginMetrics pluginMetrics;
    private final Counter positiveAcknowledgementSets;
    private final Counter negativeAcknowledgementSets;
    private final Counter noDataExtendLeaseCount;
    private final Counter giveupPartitionCount;

    public StreamCheckpointManager(final StreamCheckpointer streamCheckpointer,
                                   final boolean isAcknowledgmentEnabled,
                                   final AcknowledgementSetManager acknowledgementSetManager,
                                   final Runnable stopStreamRunnable,
                                   final Duration acknowledgmentTimeout,
                                   final EngineType engineType,
                                   final PluginMetrics pluginMetrics) {
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.streamCheckpointer = streamCheckpointer;
        this.isAcknowledgmentEnabled = isAcknowledgmentEnabled;
        this.stopStreamRunnable = stopStreamRunnable;
        this.acknowledgmentTimeout = acknowledgmentTimeout;
        this.engineType = engineType;
        this.pluginMetrics = pluginMetrics;
        executorService = Executors.newSingleThreadExecutor();

        this.positiveAcknowledgementSets = pluginMetrics.counter(POSITIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME);
        this.negativeAcknowledgementSets = pluginMetrics.counter(NEGATIVE_ACKNOWLEDGEMENT_SET_METRIC_NAME);
        this.noDataExtendLeaseCount = pluginMetrics.counter(NO_DATA_EXTEND_LEASE_COUNT);
        this.giveupPartitionCount = pluginMetrics.counter(GIVE_UP_PARTITION_COUNT);
    }

    public void start() {
        executorService.submit(this::runCheckpointing);
    }

    void runCheckpointing() {
        ChangeEventStatus currentChangeEventStatus;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (changeEventStatuses.isEmpty()) {
                    LOG.debug("No records processed. Extend the lease on stream partition.");
                    noDataExtendLeaseCount.increment();
                    streamCheckpointer.extendLease();
                } else {
                    if (isAcknowledgmentEnabled) {
                        ChangeEventStatus lastChangeEventStatus = null;
                        currentChangeEventStatus = changeEventStatuses.peek();
                        while (currentChangeEventStatus != null && currentChangeEventStatus.isPositiveAcknowledgment()) {
                            lastChangeEventStatus = currentChangeEventStatus;
                            currentChangeEventStatus = changeEventStatuses.poll();
                        }

                        if (lastChangeEventStatus != null) {
                            checkpoint(engineType, lastChangeEventStatus);
                        }

                        // If negative ack is seen, give up partition and exit loop to stop processing stream
                        if (currentChangeEventStatus != null && currentChangeEventStatus.isNegativeAcknowledgment()) {
                            LOG.info("Received negative acknowledgement for change event at {}. Will restart from most recent checkpoint", currentChangeEventStatus.getBinlogCoordinate());
                            streamCheckpointer.giveUpPartition();
                            giveupPartitionCount.increment();
                            break;
                        }
                    } else {
                        int changeEventCount = 0;
                        do {
                            currentChangeEventStatus = changeEventStatuses.poll();
                            changeEventCount++;
                            // In case queue are populated faster than the poll, checkpoint when reaching certain count
                            if (changeEventCount % CHANGE_EVENT_COUNT_PER_CHECKPOINT_BATCH == 0) {
                                checkpoint(engineType, currentChangeEventStatus);
                            }
                        } while (!changeEventStatuses.isEmpty());
                        checkpoint(engineType, currentChangeEventStatus);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while checkpointing. The stream processing will start from previous checkpoint.", e);
                break;
            }

            try {
                Thread.sleep(REGULAR_CHECKPOINT_INTERVAL_MILLIS);
            } catch (InterruptedException ex) {
                break;
            }
        }

        stopStreamRunnable.run();
        stop();
    }

    public void stop() {
        executorService.shutdownNow();
    }

    public ChangeEventStatus saveChangeEventsStatus(BinlogCoordinate binlogCoordinate, long recordCount) {
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli(), recordCount);
        changeEventStatuses.add(changeEventStatus);
        return changeEventStatus;
    }

    public ChangeEventStatus saveChangeEventsStatus(LogSequenceNumber logSequenceNumber, long recordCount) {
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(logSequenceNumber, Instant.now().toEpochMilli(), recordCount);
        changeEventStatuses.add(changeEventStatus);
        return changeEventStatus;
    }

    public AcknowledgementSet createAcknowledgmentSet(BinlogCoordinate binlogCoordinate, long recordCount) {
        LOG.debug("Create acknowledgment set for events receive prior to {}", binlogCoordinate);
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli(), recordCount);
        changeEventStatuses.add(changeEventStatus);
        return getAcknowledgementSet(changeEventStatus);
    }

    public AcknowledgementSet createAcknowledgmentSet(LogSequenceNumber logSequenceNumber, long recordCount) {
        LOG.debug("Create acknowledgment set for events receive prior to {}", logSequenceNumber);
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(logSequenceNumber, Instant.now().toEpochMilli(), recordCount);
        changeEventStatuses.add(changeEventStatus);
        return getAcknowledgementSet(changeEventStatus);
    }

    private AcknowledgementSet getAcknowledgementSet(ChangeEventStatus changeEventStatus) {
        return acknowledgementSetManager.create((result) -> {
            if (result) {
                positiveAcknowledgementSets.increment();
                changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.POSITIVE_ACK);
            } else {
                negativeAcknowledgementSets.increment();
                changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.NEGATIVE_ACK);
            }
        }, acknowledgmentTimeout);
    }

    private void checkpoint(final EngineType engineType, final ChangeEventStatus changeEventStatus) {
        LOG.debug("Checkpoint at {} with record count {}. ", engineType.isMySql() ?
                changeEventStatus.getBinlogCoordinate() : changeEventStatus.getLogSequenceNumber(),
                changeEventStatus.getRecordCount());
        streamCheckpointer.checkpoint(engineType, changeEventStatus);
    }

    //VisibleForTesting
    ConcurrentLinkedQueue<ChangeEventStatus> getChangeEventStatuses() {
        return changeEventStatuses;
    }
}
