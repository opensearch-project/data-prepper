package org.opensearch.dataprepper.plugins.mongo.stream;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.mongo.model.CheckpointStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StreamAcknowledgementManager {
    private static final Logger LOG = LoggerFactory.getLogger(StreamAcknowledgementManager.class);
    private final ConcurrentLinkedQueue<CheckpointStatus> checkpoints = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, CheckpointStatus> ackStatus = new ConcurrentHashMap<>();

    private final AcknowledgementSetManager acknowledgementSetManager;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;

    private final Duration partitionAcknowledgmentTimeout;
    private final int acknowledgementMonitorWaitTimeInMs;
    private final int checkPointIntervalInMs;
    private final ExecutorService executorService;

    private boolean enableAcknowledgement = false;

    public StreamAcknowledgementManager(final AcknowledgementSetManager acknowledgementSetManager,
                                        final DataStreamPartitionCheckpoint partitionCheckpoint,
                                        final Duration partitionAcknowledgmentTimeout,
                                        final int acknowledgementMonitorWaitTimeInMs,
                                        final int checkPointIntervalInMs) {
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.partitionCheckpoint = partitionCheckpoint;
        this.partitionAcknowledgmentTimeout = partitionAcknowledgmentTimeout;
        this.acknowledgementMonitorWaitTimeInMs = acknowledgementMonitorWaitTimeInMs;
        this.checkPointIntervalInMs = checkPointIntervalInMs;
        executorService = Executors.newSingleThreadExecutor();
    }

    void init() {
        enableAcknowledgement = true;
        final Thread currentThread = Thread.currentThread();
        executorService.submit(() -> monitorCheckpoints(executorService, currentThread));
    }

    private void monitorCheckpoints(final ExecutorService executorService, final Thread parentThread) {
        long lastCheckpointTime = System.currentTimeMillis();
        CheckpointStatus lastCheckpointStatus = null;
        while (!Thread.currentThread().isInterrupted()) {
            final CheckpointStatus checkpointStatus = checkpoints.peek();
            if (checkpointStatus != null) {
                if (checkpointStatus.isAcknowledged()) {
                    lastCheckpointStatus = checkpoints.poll();
                    ackStatus.remove(checkpointStatus.getResumeToken());
                    if (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs) {
                        LOG.debug("Perform regular checkpointing for resume token {} at record count {}", checkpointStatus.getResumeToken(), checkpointStatus.getRecordCount());
                        partitionCheckpoint.checkpoint(checkpointStatus.getResumeToken(), checkpointStatus.getRecordCount());
                        lastCheckpointTime = System.currentTimeMillis();
                    }
                } else {
                    LOG.debug("Checkpoint not complete for resume token {}", checkpointStatus.getResumeToken());
                    final Duration ackWaitDuration = Duration.between(Instant.ofEpochMilli(checkpointStatus.getCreateTimestamp()), Instant.now());
                    // Acknowledgement not received for the checkpoint after twice ack wait time
                    if (ackWaitDuration.getSeconds() > partitionAcknowledgmentTimeout.getSeconds() * 2) {
                        // Give up partition and should interrupt parent thread to stop processing stream
                        if (lastCheckpointStatus != null && lastCheckpointStatus.isAcknowledged()) {
                            partitionCheckpoint.checkpoint(lastCheckpointStatus.getResumeToken(), lastCheckpointStatus.getRecordCount());
                        }
                        LOG.warn("Acknowledgement not received for the checkpoint {} past wait time. Giving up partition.", checkpointStatus.getResumeToken());
                        partitionCheckpoint.giveUpPartition();
                        Thread.currentThread().interrupt();
                    }
                }
            }

            try {
                Thread.sleep(acknowledgementMonitorWaitTimeInMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        parentThread.interrupt();
        executorService.shutdown();
    }

    Optional<AcknowledgementSet> createAcknowledgementSet(final String resumeToken, final long recordNumber) {
        if (!enableAcknowledgement) {
            return Optional.empty();
        }

        final CheckpointStatus checkpointStatus = new CheckpointStatus(resumeToken, recordNumber, Instant.now().toEpochMilli());
        checkpoints.add(checkpointStatus);
        ackStatus.put(resumeToken, checkpointStatus);
        return Optional.of(acknowledgementSetManager.create((result) -> {
            if (result) {
                final CheckpointStatus ackCheckpointStatus = ackStatus.get(resumeToken);
                ackCheckpointStatus.setAcknowledgedTimestamp(Instant.now().toEpochMilli());
                ackCheckpointStatus.setAcknowledged(true);
                LOG.debug("Received acknowledgment of completion from sink for checkpoint {}", resumeToken);
            } else {
                LOG.warn("Negative acknowledgment received for checkpoint {}, resetting checkpoint", resumeToken);
                // default CheckpointStatus acknowledged value is false. The monitorCheckpoints method will time out
                // and reprocess stream from last successful checkpoint in the order.
            }
        }, partitionAcknowledgmentTimeout));
    }

    void shutdown() {
        executorService.shutdown();
    }

    @VisibleForTesting
    ConcurrentHashMap<String, CheckpointStatus> getAcknowledgementStatus() {
        return ackStatus;
    }

    @VisibleForTesting
    ConcurrentLinkedQueue<CheckpointStatus> getCheckpoints() {
        return checkpoints;
    }
}
