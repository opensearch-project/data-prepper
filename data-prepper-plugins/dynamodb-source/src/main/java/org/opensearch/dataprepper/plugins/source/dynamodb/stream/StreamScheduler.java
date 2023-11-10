/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * A scheduler to manage all the stream related work in one place
 */
public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);

    /**
     * Max number of shards each node can handle in parallel
     */
    private static final int MAX_JOB_COUNT = 250;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 15_000;

    /**
     * Add a delay of getting child shards when the parent finished.
     */
    private static final int DELAY_TO_GET_CHILD_SHARDS_MILLIS = 1_500;

    static final String ACTIVE_CHANGE_EVENT_CONSUMERS = "activeChangeEventConsumers";

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);
    private final EnhancedSourceCoordinator coordinator;
    private final ShardConsumerFactory consumerFactory;
    private final ExecutorService executor;
    private final ShardManager shardManager;
    private final PluginMetrics pluginMetrics;
    private final AtomicLong activeChangeEventConsumers;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final DynamoDBSourceConfig dynamoDBSourceConfig;


    public StreamScheduler(final EnhancedSourceCoordinator coordinator,
                           final ShardConsumerFactory consumerFactory,
                           final ShardManager shardManager,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final DynamoDBSourceConfig dynamoDBSourceConfig) {
        this.coordinator = coordinator;
        this.shardManager = shardManager;
        this.consumerFactory = consumerFactory;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.dynamoDBSourceConfig = dynamoDBSourceConfig;

        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);
        activeChangeEventConsumers = pluginMetrics.gauge(ACTIVE_CHANGE_EVENT_CONSUMERS, new AtomicLong());
    }

    private void processStreamPartition(StreamPartition streamPartition) {
        final boolean acknowledgmentsEnabled = dynamoDBSourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;

        if (acknowledgmentsEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result == true) {
                    LOG.info("Received acknowledgment of completion from sink for shard {}", streamPartition.getShardId());
                    completeConsumer(streamPartition).accept(null, null);
                } else {
                    LOG.warn("Negative acknowledgment received for shard {}, it will be retried", streamPartition.getShardId());
                    coordinator.giveUpPartition(streamPartition);
                }
            }, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        }

        Runnable shardConsumer = consumerFactory.createConsumer(streamPartition, acknowledgementSet, dynamoDBSourceConfig.getShardAcknowledgmentTimeout());
        if (shardConsumer != null) {

            CompletableFuture runConsumer = CompletableFuture.runAsync(shardConsumer, executor);

            if (acknowledgmentsEnabled) {
                runConsumer.whenComplete((v, ex) -> numOfWorkers.decrementAndGet());
            } else {
                runConsumer.whenComplete(completeConsumer(streamPartition));
            }
            numOfWorkers.incrementAndGet();
        } else {
            // If failed to create a new consumer.
            coordinator.completePartition(streamPartition);
        }
    }

    @Override
    public void run() {
        LOG.debug("Stream Scheduler start to run...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (numOfWorkers.get() < MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
                    if (sourcePartition.isPresent()) {
                        activeChangeEventConsumers.incrementAndGet();
                        StreamPartition streamPartition = (StreamPartition) sourcePartition.get();
                        processStreamPartition(streamPartition);
                        activeChangeEventConsumers.decrementAndGet();
                    }
                }

                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("InterruptedException occurred");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing a shard for streams, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        // Should Stop
        LOG.warn("Stream Scheduler is interrupted, looks like shutdown has triggered");

        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        ShardConsumer.stopAll();
        executor.shutdown();
    }

    private BiConsumer completeConsumer(StreamPartition streamPartition) {
        return (v, ex) -> {
            if (!dynamoDBSourceConfig.isAcknowledgmentsEnabled()) {
                numOfWorkers.decrementAndGet();
            }
            if (ex == null) {
                LOG.info("Shard consumer for {} is completed", streamPartition.getShardId());
                LOG.debug("Start creating new stream partitions for Child Shards");

                try {
                    // Add a delay as the Child shards may not be ready yet.
                    Thread.sleep(DELAY_TO_GET_CHILD_SHARDS_MILLIS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                List<String> childShardIds = shardManager.getChildShardIds(streamPartition.getStreamArn(), streamPartition.getShardId());
                LOG.info("{} child shards for {} have been found", childShardIds.size(), streamPartition.getShardId());

                createStreamPartitions(streamPartition.getStreamArn(), childShardIds);
                LOG.info("Creation of all child shards partitions is completed");
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
