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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.google.common.math.LongMath.pow;
import static com.google.common.primitives.Longs.min;
import static java.lang.Math.max;

/**
 * A scheduler to manage all the stream related work in one place
 */
public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);
    private static final Logger SHARD_COUNT_LOGGER = LoggerFactory.getLogger("ShardCountLogger");

    private static final Random RANDOM = new Random();

    /**
     * Max number of shards each node can handle in parallel
     */
    private static final int MAX_JOB_COUNT = 150;

    static final Duration STARTING_BACKOFF = Duration.ofMillis(500);
    static final Duration MAX_BACKOFF_WITH_SHARDS = Duration.ofSeconds(15);
    static final Duration MAX_BACKOFF_NO_SHARDS_ACQUIRED = Duration.ofSeconds(15);
    static final int BACKOFF_RATE = 2;
    static final Duration MAX_JITTER = Duration.ofSeconds(2);
    static final Duration MIN_JITTER = Duration.ofSeconds(-2);

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
    private int noAvailableShardsCount = 0;


    public StreamScheduler(final EnhancedSourceCoordinator coordinator,
                           final ShardConsumerFactory consumerFactory,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final DynamoDBSourceConfig dynamoDBSourceConfig) {
        this.coordinator = coordinator;
        this.consumerFactory = consumerFactory;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.dynamoDBSourceConfig = dynamoDBSourceConfig;

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
                    Thread.sleep(calculateBackoffToAcquireNextShard(noAvailableShardsCount));
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

    private long calculateBackoffToAcquireNextShard(final int noAvailableShardCount) {

        // When no shards are available to process we backoff exponentially based on how many consecutive attempts have been made without getting a shard
        // This limits calls to the coordination store
        if (noAvailableShardCount > 0) {
            if (noAvailableShardCount % 10 == 0) {
                LOG.info("No shards acquired after {} attempts", noAvailableShardCount);
            }

            final long jitterMillis = MIN_JITTER.toMillis() + RANDOM.nextInt((int) (MAX_JITTER.toMillis() - MIN_JITTER.toMillis() + 1));
            return max(1, min(STARTING_BACKOFF.toMillis() * pow(BACKOFF_RATE, noAvailableShardCount - 1) + jitterMillis, MAX_BACKOFF_NO_SHARDS_ACQUIRED.toMillis()));
        }

        // When shards are being acquired we backoff linearly based on how many shards this node is actively processing, to encourage a fast start but still a balance of shards between nodes
        return min(MAX_BACKOFF_WITH_SHARDS.toMillis(), numOfWorkers.get() * STARTING_BACKOFF.toMillis());
    }

}