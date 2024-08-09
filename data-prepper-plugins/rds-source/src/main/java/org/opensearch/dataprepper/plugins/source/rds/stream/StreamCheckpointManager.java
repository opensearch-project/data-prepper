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
    static final int REGULAR_CHECKPOINT_INTERVAL_MILLIS = 60_000;

    private final ConcurrentLinkedQueue<ChangeEventStatus> changeEventStatuses = new ConcurrentLinkedQueue<>();
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
        ChangeEventStatus currentChangeEventStatus;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (changeEventStatuses.isEmpty()) {
                    LOG.debug("No records processed. Extend the lease on stream partition.");
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
                            streamCheckpointer.checkpoint(lastChangeEventStatus.getBinlogCoordinate());
                        }

                        // If negative ack is seen, give up partition and exit loop to stop processing stream
                        if (currentChangeEventStatus != null && currentChangeEventStatus.isNegativeAcknowledgment()) {
                            LOG.info("Received negative acknowledgement for change event at {}. Will restart from most recent checkpoint", currentChangeEventStatus.getBinlogCoordinate());
                            streamCheckpointer.giveUpPartition();
                            break;
                        }
                    } else {
                        do {
                            currentChangeEventStatus = changeEventStatuses.poll();
                        } while (!changeEventStatuses.isEmpty());
                        streamCheckpointer.checkpoint(currentChangeEventStatus.getBinlogCoordinate());
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

    public ChangeEventStatus saveChangeEventsStatus(BinlogCoordinate binlogCoordinate) {
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli());
        changeEventStatuses.add(changeEventStatus);
        return changeEventStatus;
    }

    public AcknowledgementSet createAcknowledgmentSet(BinlogCoordinate binlogCoordinate) {
        LOG.debug("Create acknowledgment set for events receive prior to {}", binlogCoordinate);
        final ChangeEventStatus changeEventStatus = new ChangeEventStatus(binlogCoordinate, Instant.now().toEpochMilli());
        changeEventStatuses.add(changeEventStatus);
        return acknowledgementSetManager.create((result) -> {
           if (result) {
               changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.POSITIVE_ACK);
           } else {
               changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.NEGATIVE_ACK);
           }
        }, acknowledgmentTimeout);
    }

    //VisibleForTesting
    ConcurrentLinkedQueue<ChangeEventStatus> getChangeEventStatuses() {
        return changeEventStatuses;
    }
}
