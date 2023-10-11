/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * A scheduler to manage all the stream related work in one place
 */
public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    private static final int MAX_JOB_COUNT = 50;
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 30_000;

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);
    private final EnhancedSourceCoordinator coordinator;
    private final ShardConsumerFactory consumerFactory;
    private final ExecutorService executor;
    private final ShardManager shardManager;


    public StreamScheduler(EnhancedSourceCoordinator coordinator, ShardConsumerFactory consumerFactory, ShardManager shardManager) {
        this.coordinator = coordinator;
        this.shardManager = shardManager;
        this.consumerFactory = consumerFactory;

        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);

    }

    private void processStreamPartition(StreamPartition streamPartition) {
        Runnable shardConsumer = consumerFactory.createConsumer(streamPartition);
        if (shardConsumer != null) {
            CompletableFuture runConsumer = CompletableFuture.runAsync(shardConsumer, executor);
            runConsumer.whenComplete(completeConsumer(streamPartition));
            numOfWorkers.incrementAndGet();
        } else {
            // If failed to create a new consumer.
            coordinator.completePartition(streamPartition);
        }
    }

    @Override
    public void run() {
        LOG.debug("Stream Scheduler start to run...");
        while (!Thread.interrupted()) {
            if (numOfWorkers.get() < MAX_JOB_COUNT) {
                final Optional<SourcePartition> sourcePartition = coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    StreamPartition streamPartition = (StreamPartition) sourcePartition.get();
                    processStreamPartition(streamPartition);
                }
            }

            try {
                Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
            } catch (final InterruptedException e) {
                LOG.info("InterruptedException occurred");
                break;
            }
        }
        // Should Stop
        LOG.debug("Stream Scheduler is interrupted, looks like shutdown has triggered");

        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        ShardConsumer.stopAll();
        executor.shutdown();
    }

    private BiConsumer completeConsumer(StreamPartition streamPartition) {
        return (v, ex) -> {
            numOfWorkers.decrementAndGet();
            if (ex == null) {
                LOG.debug("Shard consumer is completed");
                LOG.debug("Start creating new stream partitions for Child Shards");

                List<String> childShardIds = shardManager.getChildShardIds(streamPartition.getStreamArn(), streamPartition.getShardId());
                LOG.debug("Child Ids Retrieved: {}", childShardIds);

                createStreamPartitions(streamPartition.getStreamArn(), childShardIds);
                LOG.debug("Create child shard completed");
                // Finally mask the partition as completed.
                coordinator.completePartition(streamPartition);

            } else {
                // Do nothing
                // The consumer must have already done one last checkpointing.
                LOG.debug("Shard consumer completed with exception");
                LOG.error(ex.toString());
                coordinator.giveUpPartition(streamPartition);


            }
        };
    }

    private void createStreamPartitions(String streamArn, List<String> shardIds) {
        shardIds.forEach(
                shardId -> {
                    StreamPartition partition = new StreamPartition(streamArn, shardId, Optional.of(new StreamProgressState()));
                    coordinator.createPartition(partition);
                }
        );
    }


}
