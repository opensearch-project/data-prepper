/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamCheckpointManager {
    private static final Logger LOG = LoggerFactory.getLogger(StreamCheckpointManager.class);
    static final int QUEUE_MONITORING_INTERVAL_MILLIS = 15_000;
    static final int REGULAR_CHECKPOINT_INTERVAL_MILLIS = 60_000;

    private final ConcurrentLinkedQueue<RowChangeEventStatus> changeEventStatuses = new ConcurrentLinkedQueue<>();
    private final StreamCheckpointer streamCheckpointer;
    private final ExecutorService executorService;
    private final Runnable stopStreamRunnable;
    private final boolean isAcknowledgmentEnabled;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Duration acknowledgmentTimeout;

    public StreamCheckpointManager(final StreamCheckpointer streamCheckpointer,
                                   final boolean isAcknowledgmentEnabled,
                                   final AcknowledgementSetManager acknowledgementSetManager,
                                   final Runnable stopStreamRunnable,
                                   final Duration acknowledgmentTimeout) {
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.streamCheckpointer = streamCheckpointer;
        this.isAcknowledgmentEnabled = isAcknowledgmentEnabled;
        this.stopStreamRunnable = stopStreamRunnable;
        this.acknowledgmentTimeout = acknowledgmentTimeout;
        executorService = Executors.newSingleThreadExecutor();
    }

    public void start() {
        executorService.submit(this::runCheckpointing);
    }

    void runCheckpointing() {
        long lastCheckpointTime = System.currentTimeMillis();
        RowChangeEventStatus changeEventStatus;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (!changeEventStatuses.isEmpty()) {
                    if (isAcknowledgmentEnabled) {
                        if (System.currentTimeMillis() - lastCheckpointTime > REGULAR_CHECKPOINT_INTERVAL_MILLIS) {
                            RowChangeEventStatus lastChangeEventStatus = null;
                            changeEventStatus = changeEventStatuses.peek();
                            while (changeEventStatus.isPositiveAcknowledgment()) {
                                lastChangeEventStatus = changeEventStatus;
                                changeEventStatus = changeEventStatuses.poll();
                            }
                            if (lastChangeEventStatus != null) {
                                streamCheckpointer.checkpoint(lastChangeEventStatus.getBinlogCoordinate());
                            } else {
                                // The ChangeEventStatus at the front of the queue is not positively ack'ed
                                if (changeEventStatus.isNegativeAcknowledgment()) {
                                    LOG.info("Received negative acknowledgement for change event at {}. Will restart from most recent checkpoint", changeEventStatus.getBinlogCoordinate());
                                    //
                                    streamCheckpointer.giveUpPartition();
                                    break;
                                }
                            }
                        }
                    } else {
                        if (System.currentTimeMillis() - lastCheckpointTime > REGULAR_CHECKPOINT_INTERVAL_MILLIS) {
                            do {
                                changeEventStatus = changeEventStatuses.poll();
                            } while (!changeEventStatuses.isEmpty());
                            streamCheckpointer.checkpoint(changeEventStatus.getBinlogCoordinate());
                            lastCheckpointTime = System.currentTimeMillis();
                        }
                    }
                } else {
                    if (System.currentTimeMillis() - lastCheckpointTime > REGULAR_CHECKPOINT_INTERVAL_MILLIS) {
                        LOG.debug("No records processed. Extend the lease on stream partition.");
                        streamCheckpointer.extendLease();
                        lastCheckpointTime = System.currentTimeMillis();
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while checkpointing. The stream processing will start from previous checkpoint.", e);
                break;
            }

            try {
                Thread.sleep(QUEUE_MONITORING_INTERVAL_MILLIS);
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

    public void saveChangeEventsStatus(BinlogCoordinate binlogCoordinate) {
        final RowChangeEventStatus changeEventStatus = new RowChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli());
        changeEventStatuses.add(changeEventStatus);
    }

    public AcknowledgementSet createAcknowledgmentSet(BinlogCoordinate binlogCoordinate) {
        LOG.debug("Create acknowledgment set for events receive prior to {}", binlogCoordinate);
        final RowChangeEventStatus changeEventStatus = new RowChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli());
        changeEventStatuses.add(changeEventStatus);
        return acknowledgementSetManager.create((result) -> {
           if (result) {
               changeEventStatus.setAcknowledgmentStatus(RowChangeEventStatus.AcknowledgmentStatus.POSITIVE_ACK);
           } else {
               changeEventStatus.setAcknowledgmentStatus(RowChangeEventStatus.AcknowledgmentStatus.NEGATIVE_ACK);
           }
        }, acknowledgmentTimeout);
    }

    //VisibleForTesting
    ConcurrentLinkedQueue<RowChangeEventStatus> getChangeEventStatuses() {
        return changeEventStatuses;
    }
}
