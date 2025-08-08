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
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.BackoffCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * A scheduler to manage all the stream related work in one place
 */
public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);
    private static final Logger SHARD_COUNT_LOGGER = LoggerFactory.getLogger("org.opensearch.dataprepper.plugins.source.dynamodb.stream.ShardCountLogger");

    /**
     * Max number of shards each node can handle in parallel
     */
    private static final int MAX_JOB_COUNT = 150;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 15_000;

    static final String ACTIVE_CHANGE_EVENT_CONSUMERS = "activeChangeEventConsumers";
    static final String SHARDS_IN_PROCESSING = "activeShardsInProcessing";

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);
    private final EnhancedSourceCoordinator coordinator;
    private final ShardConsumerFactory consumerFactory;
    private final ExecutorService executor;
    private final PluginMetrics pluginMetrics;
    private final AtomicLong activeChangeEventConsumers;
    private final AtomicLong shardsInProcessing;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final DynamoDBSourceConfig dynamoDBSourceConfig;
    private final BackoffCalculator backoffCalculator;
    private int noAvailableShardsCount = 0;


    public StreamScheduler(final EnhancedSourceCoordinator coordinator,
                           final ShardConsumerFactory consumerFactory,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final DynamoDBSourceConfig dynamoDBSourceConfig,
                           final BackoffCalculator backoffCalculator) {
        this.coordinator = coordinator;
        this.consumerFactory = consumerFactory;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.dynamoDBSourceConfig = dynamoDBSourceConfig;
        this.backoffCalculator = backoffCalculator;

        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);
        activeChangeEventConsumers = pluginMetrics.gauge(ACTIVE_CHANGE_EVENT_CONSUMERS, new AtomicLong());
        shardsInProcessing = pluginMetrics.gauge(SHARDS_IN_PROCESSING, new AtomicLong());
    }

    private void processStreamPartition(StreamPartition streamPartition) {
        final boolean acknowledgmentsEnabled = dynamoDBSourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;

        if (acknowledgmentsEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result) {
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
                runConsumer.whenComplete((v, ex) -> {
                    numOfWorkers.decrementAndGet();
                    if (ex != null) {
                        LOG.error(NOISY, "Received exception while processing shard {}, giving up this shard for reprocessing: {}",
                                streamPartition.getShardId(), ex);
                        coordinator.giveUpPartition(streamPartition);
                    }
                    if (numOfWorkers.get() == 0) {
                        activeChangeEventConsumers.decrementAndGet();
                    }
                    shardsInProcessing.decrementAndGet();
                });
            } else {
                runConsumer.whenComplete(completeConsumer(streamPartition));
            }
            numOfWorkers.incrementAndGet();
            if (numOfWorkers.get() % 10 == 0) {
                SHARD_COUNT_LOGGER.info("Actively processing {} shards", numOfWorkers.get());
            }

            if (numOfWorkers.get() >= 1) {
                activeChangeEventConsumers.incrementAndGet();
            }
            shardsInProcessing.incrementAndGet();
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
                        StreamPartition streamPartition = (StreamPartition) sourcePartition.get();
                        processStreamPartition(streamPartition);
                        noAvailableShardsCount = 0;
                    } else {
                        noAvailableShardsCount++;
                    }
                }

                try {
                    Thread.sleep(backoffCalculator.calculateBackoffToAcquireNextShard(noAvailableShardsCount, numOfWorkers));
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
                if (numOfWorkers.get() == 0) {
                    activeChangeEventConsumers.decrementAndGet();
                }
                shardsInProcessing.decrementAndGet();
            }
            if (ex == null) {
                LOG.info("Shard consumer for {} is completed", streamPartition.getShardId());
                coordinator.completePartition(streamPartition);
            } else {
                // Do nothing
                // The consumer must have already done one last checkpointing.
                LOG.error("Received an exception while processing shard {}, giving up shard: {}", streamPartition.getShardId(), ex);
                coordinator.giveUpPartition(streamPartition);
            }
        };
    }

}