package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.opensearch.dataprepper.plugins.source.dynamodb.model.ShardCheckpointStatus;

public class ShardAcknowledgementManager {
    private static final Logger LOG = LoggerFactory.getLogger(ShardAcknowledgementManager.class);

    private static final String NULL_SEQUENCE_NUMBER = "null";

    private static final long WAIT_FOR_ACKNOWLEDGMENTS_TIMEOUT = 10L;

    static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(2);

    private final DynamoDBSourceConfig dynamoDBSourceConfig;
    private final Map<StreamPartition, ConcurrentLinkedQueue<ShardCheckpointStatus>> checkpoints = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<StreamPartition, ConcurrentHashMap<String, ShardCheckpointStatus>> ackStatuses = new ConcurrentHashMap<>();

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final EnhancedSourceCoordinator sourceCoordinator;

    private final ExecutorService executorService;
    private final List<StreamPartition> partitionsToRemove;
    private boolean shutdownTriggered;

    private Instant lastCheckpointTime;

    public ShardAcknowledgementManager(final AcknowledgementSetManager acknowledgementSetManager,
                                       final EnhancedSourceCoordinator sourceCoordinator,
                                       final DynamoDBSourceConfig dynamoDBSourceConfig,
                                       final Consumer<StreamPartition> stopWorkerConsumer
    ) {
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceCoordinator = sourceCoordinator;
        this.dynamoDBSourceConfig = dynamoDBSourceConfig;
        this.executorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("dynamodb-shard-ack-monitor"));
        this.partitionsToRemove = Collections.synchronizedList(new ArrayList<>());
        this.lastCheckpointTime = Instant.now();
        
        // Start monitoring acknowledgments in the constructor
        executorService.submit(() -> monitorAcknowledgments(stopWorkerConsumer));
    }

    void monitorAcknowledgments(final Consumer<StreamPartition> stopWorkerConsumer) {
        while (!Thread.currentThread().isInterrupted()) {
            boolean exit = runMonitorAcknowledgmentLoop(stopWorkerConsumer);
            if (exit) {
                break;
            }
        }

        LOG.info("Exiting acknowledgment manager");
    }

    boolean runMonitorAcknowledgmentLoop(final Consumer<StreamPartition> stopWorkerConsumer) {
        removePartitions();
        if (shutdownTriggered && checkpoints.isEmpty()) {
            LOG.info("Shutdown was triggered and not waiting on any acknowledgments, exiting cleanly");
            return true;
        }

        for (final StreamPartition streamPartition : checkpoints.keySet()) {
            try {
                final StreamProgressState streamProgressState = streamPartition.getProgressState().orElseThrow();
                final ConcurrentLinkedQueue<ShardCheckpointStatus> checkpointStatuses = checkpoints.get(streamPartition);
                ShardCheckpointStatus latestCheckpointForShard = null;
                boolean gaveUpPartition = false;
                while (!checkpointStatuses.isEmpty()) {
                    updateOwnershipForAllShardPartitions();

                    if (checkpointStatuses.peek().isPositiveAcknowledgement()) {
                        latestCheckpointForShard = checkpointStatuses.poll();
                    } else if (checkpointStatuses.peek().isNegativeAcknowledgement()
                        || checkpointStatuses.peek().isExpired(dynamoDBSourceConfig.getShardAcknowledgmentTimeout())) {
                        handleFailure(streamPartition, streamProgressState, latestCheckpointForShard);
                        gaveUpPartition = true;

                        if (checkpointStatuses.peek().isNegativeAcknowledgement()) {
                            LOG.warn("Received negative acknowledgment for partition {} with sequence number {}, giving up partition",
                                streamPartition.getPartitionKey(), checkpointStatuses.peek().getSequenceNumber());
                        } else {
                            LOG.warn("Acknowledgment timed out for partition {} with sequence number {}, giving up partition",
                                streamPartition.getPartitionKey(), checkpointStatuses.peek().getSequenceNumber());
                        }

                        stopWorkerConsumer.accept(streamPartition);
                        break;
                    } else {
                        break;
                    }
                }

                if (!gaveUpPartition) {
                    updateOwnershipForAllShardPartitions();
                }

                if (gaveUpPartition || latestCheckpointForShard == null) {
                    continue;
                }

                if (latestCheckpointForShard.isFinalAcknowledgmentForPartition()) {
                    handleCompletedShard(streamPartition);
                } else {
                    streamProgressState.setSequenceNumber(Objects.equals(latestCheckpointForShard.getSequenceNumber(), NULL_SEQUENCE_NUMBER) ? null : latestCheckpointForShard.getSequenceNumber());
                    sourceCoordinator.saveProgressStateForPartition(streamPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
                    LOG.debug("Checkpointed shard {} with latest sequence number acknowledged {}", streamPartition.getShardId(), latestCheckpointForShard.getSequenceNumber());
                }
            } catch (final Exception e) {
                LOG.error("Received exception while monitoring acknowledgments for stream partition {}", streamPartition.getPartitionKey(), e);
            }
        }

        return false;
    }

    public AcknowledgementSet createAcknowledgmentSet(
        final StreamPartition streamPartition,
        final String sequenceNumber,
        final boolean isFinalSetForPartition) {
        final String sequenceNumberNoNull = sequenceNumber == null ? NULL_SEQUENCE_NUMBER : sequenceNumber;
        final ShardCheckpointStatus shardCheckpointStatus = new ShardCheckpointStatus(sequenceNumber, Instant.now().toEpochMilli(), isFinalSetForPartition);
        checkpoints.computeIfAbsent(streamPartition, segment -> new ConcurrentLinkedQueue<>()).add(shardCheckpointStatus);
        ackStatuses.computeIfAbsent(streamPartition, segment -> new ConcurrentHashMap<>());
        ackStatuses.get(streamPartition).put(sequenceNumberNoNull, shardCheckpointStatus);

        return acknowledgementSetManager.create((result) -> {
            if (ackStatuses.containsKey(streamPartition) && ackStatuses.get(streamPartition).containsKey(sequenceNumberNoNull)) {
                final ShardCheckpointStatus ackCheckpointStatus = ackStatuses.get(streamPartition).get(sequenceNumberNoNull);

                ackCheckpointStatus.setAcknowledgedTimestamp(Instant.now().toEpochMilli());

                if (result) {
                    LOG.debug("Received acknowledgment of completion from sink for partition {} with sequence number {}",
                        streamPartition.getPartitionKey(), sequenceNumberNoNull);
                    ackCheckpointStatus.setAcknowledged(ShardCheckpointStatus.AcknowledgmentStatus.POSITIVE_ACK);
                } else {
                    LOG.warn("Negative acknowledgment received for partition {} with sequence number {}",
                        streamPartition.getPartitionKey(), sequenceNumberNoNull);
                    ackCheckpointStatus.setAcknowledged(ShardCheckpointStatus.AcknowledgmentStatus.NEGATIVE_ACK);
                }
            }
        }, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
    }

    void updateOwnershipForAllShardPartitions() {
        if (Duration.between(lastCheckpointTime, Instant.now()).compareTo(CHECKPOINT_INTERVAL) > 0) {
            for (final StreamPartition streamPartition : checkpoints.keySet()) {
                if (!partitionsToRemove.contains(streamPartition)) {
                    sourceCoordinator.saveProgressStateForPartition(streamPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
                }
            }

            lastCheckpointTime = Instant.now();
        }
    }

    private void handleFailure(final StreamPartition streamPartition,
                               final StreamProgressState streamProgressState,
                               final ShardCheckpointStatus latestCheckpointForShard) {
        if (latestCheckpointForShard != null) {
            streamProgressState.setSequenceNumber(latestCheckpointForShard.getSequenceNumber());
            sourceCoordinator.saveProgressStateForPartition(streamPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        }
        partitionsToRemove.add(streamPartition);
        sourceCoordinator.giveUpPartition(streamPartition);
    }

    private void handleCompletedShard(final StreamPartition streamPartition) {
        sourceCoordinator.completePartition(streamPartition);
        partitionsToRemove.add(streamPartition);
        LOG.info("Received all acknowledgments for partition {}, marking partition as completed", streamPartition.getPartitionKey());
    }

    public void shutdown() {
        shutdownTriggered = true;
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(WAIT_FOR_ACKNOWLEDGMENTS_TIMEOUT, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void removePartitions() {
        partitionsToRemove.forEach(streamPartition -> {
            checkpoints.remove(streamPartition);
            ackStatuses.remove(streamPartition);
        });

        partitionsToRemove.clear();
    }

    public void giveUpPartition(final StreamPartition streamPartition) {
        sourceCoordinator.saveProgressStateForPartition(streamPartition, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        checkpoints.remove(streamPartition);
        ackStatuses.remove(streamPartition);
    }

    public boolean isExportDone(StreamPartition streamPartition) {
        Optional<EnhancedSourcePartition> globalPartition = sourceCoordinator.getPartition(streamPartition.getStreamArn());
        return globalPartition.isPresent();
    }
}
