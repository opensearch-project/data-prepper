/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Worker class for executing the partitioned work created while crawling a source.
 * Each SAAS source will provide their own specific source extraction logic.
 */
public class WorkerScheduler implements Runnable {

    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";
    public static final String WORKER_PARTITIONS_FAILED = "workerPartitionsFailed";
    public static final String WORKER_PARTITIONS_COMPLETED = "workerPartitionsCompleted";
    public static final String PARTITION_OWNERSHIP_RENEWAL_FAILURES = "partitionOwnershipRenewalFailures";
    public static final String ACKNOWLEDGEMENT_CALLBACK_FAILURES = "acknowledgementCallbackFailures";

    @VisibleForTesting
    static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    /** Must stay well below the coordinator's 10 minute lease so a few missed renewals do not lose the partition. */
    @VisibleForTesting
    static final Duration OWNERSHIP_RENEWAL_INTERVAL = Duration.ofMinutes(2);
    private static final Logger log = LoggerFactory.getLogger(WorkerScheduler.class);
    private static final int RETRY_BACKOFF_ON_EXCEPTION_MILLIS = 5_000;
    private static final Duration DEFAULT_SLEEP_DURATION_MILLIS = Duration.ofMillis(10000);
    private final Counter parititionsCompletedCounter;
    private final Counter parititionsFailedCounter;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final CrawlerSourceConfig sourceConfig;
    private final Crawler crawler;
    private final Buffer<Record<Event>> buffer;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Counter acknowledgementSetSuccesses;
    private final Counter acknowledgementSetFailures;
    private final Counter partitionOwnershipRenewalFailures;
    private final Counter acknowledgementCallbackFailures;
    private final String sourcePluginName;


    public WorkerScheduler(final String sourcePluginName,
                           Buffer<Record<Event>> buffer,
                           EnhancedSourceCoordinator sourceCoordinator,
                           CrawlerSourceConfig sourceConfig,
                           Crawler crawler,
                           final PluginMetrics pluginMetrics,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.crawler = crawler;
        this.buffer = buffer;
        this.sourcePluginName = sourcePluginName;

        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetSuccesses = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME);
        this.acknowledgementSetFailures = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME);
        this.parititionsCompletedCounter = pluginMetrics.counter(WORKER_PARTITIONS_COMPLETED);
        this.parititionsFailedCounter = pluginMetrics.counter(WORKER_PARTITIONS_FAILED);
        this.partitionOwnershipRenewalFailures = pluginMetrics.counter(PARTITION_OWNERSHIP_RENEWAL_FAILURES);
        this.acknowledgementCallbackFailures = pluginMetrics.counter(ACKNOWLEDGEMENT_CALLBACK_FAILURES);
    }

    @Override
    public void run() {
        log.info("Worker thread started");
        log.info("Processing Partitions");
        while (!Thread.currentThread().isInterrupted()) {
            Optional<EnhancedSourcePartition> partition = Optional.empty();
            final AtomicBoolean renewalFailed = new AtomicBoolean();
            try {
                // Get the next available partition from the coordinator
                partition = sourceCoordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE);
                if (partition.isPresent()) {
                    // Process the partition (source extraction logic)
                    processPartition(partition.get(), buffer, renewalFailed);
                    parititionsCompletedCounter.increment();

                } else {
                    log.debug("No partition available. This thread will sleep for {}", DEFAULT_SLEEP_DURATION_MILLIS);
                    try {
                        Thread.sleep(DEFAULT_SLEEP_DURATION_MILLIS.toMillis());
                    } catch (final InterruptedException e) {
                        log.info("InterruptedException occurred");
                        break;
                    }
                }
            } catch (Exception e) {
                this.parititionsFailedCounter.increment();
                // always default to backoffRetry strategy
                boolean shouldLocalRetry = true;
                if (e instanceof SaaSCrawlerException) {
                    SaaSCrawlerException saasException = (SaaSCrawlerException) e;
                    if (!saasException.isRetryable()) {
                        shouldLocalRetry = delayWorkerPartitionRetry(partition, e, renewalFailed);
                    }
                }
                if (shouldLocalRetry) {
                    backoffRetry(e);
                }
            }
        }
        log.warn("SourceItemWorker Scheduler is interrupted, looks like shutdown has triggered");
    }

    /**
     * Default behaviour of backoffRetry workerScheduler by sleeping RETRY_BACKOFF_ON_EXCEPTION_MILLIS
     * @param e - exception thrown by workerScheduler
     */
    private void backoffRetry(Exception e) {
        log.error("[Retryable Exception] Error processing partition", e);
        try {
            Thread.sleep(RETRY_BACKOFF_ON_EXCEPTION_MILLIS);
        } catch (InterruptedException ex) {
            log.warn("Thread interrupted while waiting to retry due to {}", ex.getMessage());
        }
    }

    /**
     * Delay retry on a workerPartition by X Duration (current default = 1 day) for all non-retryble exceptions up to X days (current default = 30 days)
     * @param sourcePartition - information on WorkerPartition state
     * @param ex - exception thrown by workerScheduler
     * @param renewalFailed - set once an ownership renewal for this acquisition has failed
     * @return boolean: true if we should fallback to localRetry
     */
    private boolean delayWorkerPartitionRetry(Optional<EnhancedSourcePartition> sourcePartition, Exception ex,
                                              final AtomicBoolean renewalFailed) {
        log.error("[Non-Retryable Exception] Error processing worker partition. Will delay retry with the configured duration", ex);
        try {
            SaasSourcePartition workerPartition = (SaasSourcePartition) sourcePartition.get();
            boolean shouldLocalRetry = true;
            if (workerPartition != null) {
                SaasWorkerProgressState progressState = (SaasWorkerProgressState) workerPartition.getProgressState().get();
                // TODO: ideally we should add partitionCreationTime for all type of SaasWorkerProgressState
                if (progressState instanceof DimensionalTimeSliceWorkerProgressState) {
                    DimensionalTimeSliceWorkerProgressState workerProgressState = (DimensionalTimeSliceWorkerProgressState) progressState;
                    synchronized (workerPartition) {
                        if (renewalFailed.get()) {
                            log.debug("Not updating partition {}, its ownership renewal failed", workerPartition.getPartitionKey());
                        } else {
                            updateWorkerPartition(workerProgressState.getPartitionCreationTime(), workerPartition);
                        }
                    }
                    shouldLocalRetry = false;
                }
            }

            // other SaasWorkerProgressState types (not DimensionalTimeSliceWorkerProgressState) should never use delayWorkerPartitionRetry()
            // to be safe, fallback to default retry strategy
            return shouldLocalRetry;
        } catch (Exception e) {
            log.error("Error updating workerPartition ", e);
            // on exception, do not interrupt thread and retry again
            return false;
        }
    }


    /**
     * Update the workerPartition if the partitionCreationTime <= max days to keep retrying (current default = 30 days) on nonretryable exceptions.
     * Otherwise, give up the workerPartition.
     * @param partitionCreationTime - timestamp in epoch when the worker partition was first created
     * @param workerPartition - information on WorkerPartition state
     */
    private void updateWorkerPartition(Instant partitionCreationTime, SaasSourcePartition workerPartition) {
        log.info("Updating workerPartition {}", workerPartition.getPartitionKey());
        Duration age = Duration.between(partitionCreationTime, Instant.now());
        if (age.compareTo(this.sourceConfig.getDurationToGiveUpRetry()) <= 0) {
            log.info(NOISY, "Partition {} is within or equal to the configured max duration, scheduling retry", workerPartition.getPartitionKey());
            sourceCoordinator.saveProgressStateForPartition(workerPartition, this.sourceConfig.getDurationToDelayRetry());
        } else {
            log.info("Partition {} is older than the configured max duration, giving up", workerPartition.getPartitionKey());
            sourceCoordinator.giveUpPartition(workerPartition);
        }
    }

    private void processPartition(EnhancedSourcePartition partition, Buffer<Record<Event>> buffer, final AtomicBoolean renewalFailed) {
        // Implement your source extraction logic here
        // Update the partition state or commit the partition as needed
        // Commit the partition to mark it as processed
        if (partition.getProgressState().isPresent()) {
            AcknowledgementSet acknowledgementSet = null;
            if (sourceConfig.isAcknowledgments()) {
                acknowledgementSet = createAcknowledgementSet(partition, renewalFailed);
                // Register before executePartition: the crawler completes the set, which cancels the progress check,
                // so registering afterwards would leave renewal running on an already released partition.
                acknowledgementSet.addProgressCheck(progress -> renewPartitionOwnership(partition, renewalFailed),
                        OWNERSHIP_RENEWAL_INTERVAL);
                // When acknowledgments are enabled, partition completion is handled in the acknowledgment callback
                try {
                    crawler.executePartition((SaasWorkerProgressState) partition.getProgressState().get(), buffer, acknowledgementSet);
                } catch (final RuntimeException | Error e) {
                    // Nothing will complete the set now, so renewals would hold the partition until the ack timeout.
                    acknowledgementSet.cancel();
                    throw e;
                }
            } else {
                // When acknowledgments are disabled, complete the partition immediately after execution
                crawler.executePartition((SaasWorkerProgressState) partition.getProgressState().get(), buffer, acknowledgementSet);
                sourceCoordinator.completePartition(partition);
            }
        } else {
            // If no progress state, complete the partition immediately
            sourceCoordinator.completePartition(partition);
        }
    }

    // Must never throw: a throw on the acknowledgement scheduler thread silently stops later renewals. A failed write can leave
    // the cached version ahead of the store, so after one this instance stops writing the partition and lets the lease lapse.
    private void renewPartitionOwnership(final EnhancedSourcePartition partition, final AtomicBoolean renewalFailed) {
        synchronized (partition) {
            if (renewalFailed.get()) {
                return;
            }
            try {
                sourceCoordinator.renewPartitionOwnership(partition);
            } catch (final Exception e) {
                renewalFailed.set(true);
                partitionOwnershipRenewalFailures.increment();
                log.warn("Failed to renew ownership of partition {}, it will be re-acquired after its lease lapses",
                        partition.getPartitionKey(), e);
            }
        }
    }

    @VisibleForTesting
    AcknowledgementSet createAcknowledgementSet(EnhancedSourcePartition partition, final AtomicBoolean renewalFailed) {
        return acknowledgementSetManager.create((result) -> {
            synchronized (partition) {
                if (renewalFailed.get()) {
                    log.debug("Not updating partition {} after acknowledgement, its ownership renewal failed", partition.getPartitionKey());
                    return;
                }
                // The acknowledgement callback executor discards exceptions, so they must be logged and counted here.
                try {
                    if (result) {
                        sourceCoordinator.completePartition(partition);
                        acknowledgementSetSuccesses.increment();
                        log.debug("acknowledgements received for partitionKey: {}", partition.getPartitionKey());
                    } else {
                        acknowledgementSetFailures.increment();
                        log.debug("acknowledgements received with false for partitionKey: {}", partition.getPartitionKey());
                        sourceCoordinator.giveUpPartition(partition);
                    }
                } catch (final Exception e) {
                    acknowledgementCallbackFailures.increment();
                    log.warn("Failed to update partition {} after acknowledgement result {}", partition.getPartitionKey(), result, e);
                }
            }
        }, ACKNOWLEDGEMENT_SET_TIMEOUT);
    }
}
