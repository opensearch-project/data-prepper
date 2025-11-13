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

/**
 * Worker class for executing the partitioned work created while crawling a source.
 * Each SAAS source will provide their own specific source extraction logic.
 */
public class WorkerScheduler implements Runnable {

    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";
    public static final String WORKER_PARTITIONS_FAILED = "workerPartitionsFailed";
    public static final String WORKER_PARTITIONS_COMPLETED = "workerPartitionsCompleted";
    
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofSeconds(20);
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
    }

    @Override
    public void run() {
        log.info("Worker thread started");
        log.info("Processing Partitions");
        while (!Thread.currentThread().isInterrupted()) {
            Optional<EnhancedSourcePartition> partition = Optional.empty();
            try {
                // Get the next available partition from the coordinator
                partition = sourceCoordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE);
                if (partition.isPresent()) {
                    // Process the partition (source extraction logic)
                    processPartition(partition.get(), buffer);
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
                boolean shouldUseBackoffRetry = true;
                if (e instanceof SaaSCrawlerException) {
                    SaaSCrawlerException saasException = (SaaSCrawlerException) e;
                    if (!saasException.isRetryable()) {
                        shouldUseBackoffRetry = delayRetry(partition, e);
                    }
                }
                if (shouldUseBackoffRetry) {
                    backoffRetry(e);
                }
            }
        }
        log.warn("SourceItemWorker Scheduler is interrupted, looks like shutdown has triggered");
    }

    /**
     * Default behaviour of backoff retry workerScheduler by sleeping RETRY_BACKOFF_ON_EXCEPTION_MILLIS
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
     * Delay retry by X Duration (current default = 1 day) for all non-retryble exceptions up to X days (current default = 30 days)
     * @param sourcePartition - information on WorkerPartition state
     * @param ex - exception thrown by workerScheduler
     * @return boolean: true if we should fallback to backoffRetry
     */
    private boolean delayRetry(Optional<EnhancedSourcePartition> sourcePartition, Exception ex) {
        log.error("[Non-Retryable Exception] Error processing worker partition. Will delay retry with the configured duration", ex);
        try {
            SaasSourcePartition workerPartition = (SaasSourcePartition) sourcePartition.get();
            boolean isWorkerPartitionLeaseExtended = false;
            if (workerPartition != null) {
                SaasWorkerProgressState progressState = (SaasWorkerProgressState) workerPartition.getProgressState().get();
                // TODO: ideally we should add partitionCreationTime for all type of SaasWorkerProgressState
                if (progressState instanceof DimensionalTimeSliceWorkerProgressState) {
                    DimensionalTimeSliceWorkerProgressState workerProgressState = (DimensionalTimeSliceWorkerProgressState) progressState;
                    updateWorkerPartition(workerProgressState.getPartitionCreationTime(), workerPartition);
                    isWorkerPartitionLeaseExtended = true;
                }
            }

            // other SaasWorkerProgressState types (not DimensionalTimeSliceWorkerProgressState) should never use delayRetry()
            // to be safe, fallback to default retry strategy
            if (!isWorkerPartitionLeaseExtended) {
                return true;
            }
            return false;
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
            log.info("Partition {} is within or equal to the configured max duration, scheduling retry", workerPartition.getPartitionKey());
            sourceCoordinator.saveProgressStateForPartition(workerPartition, this.sourceConfig.getDurationToDelayRetry());
        } else {
            log.info("Partition {} is older than the configured max duration, giving up", workerPartition.getPartitionKey());
            sourceCoordinator.giveUpPartition(workerPartition);
        }
    }

    private void processPartition(EnhancedSourcePartition partition, Buffer<Record<Event>> buffer) {
        // Implement your source extraction logic here
        // Update the partition state or commit the partition as needed
        // Commit the partition to mark it as processed
        if (partition.getProgressState().isPresent()) {
            AcknowledgementSet acknowledgementSet = null;
            if (sourceConfig.isAcknowledgments()) {
                acknowledgementSet = createAcknowledgementSet(partition);
                // When acknowledgments are enabled, partition completion is handled in the acknowledgment callback
                crawler.executePartition((SaasWorkerProgressState) partition.getProgressState().get(), buffer, acknowledgementSet);
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

    @VisibleForTesting
    AcknowledgementSet createAcknowledgementSet(EnhancedSourcePartition partition) {
        return acknowledgementSetManager.create((result) -> {
            if (result) {
                acknowledgementSetSuccesses.increment();
                sourceCoordinator.completePartition(partition);
                log.debug("acknowledgements received for partitionKey: {}", partition.getPartitionKey());
            } else {
                acknowledgementSetFailures.increment();
                log.debug("acknowledgements received with false for partitionKey: {}", partition.getPartitionKey());
                sourceCoordinator.giveUpPartition(partition);
            }
        }, ACKNOWLEDGEMENT_SET_TIMEOUT);
    }
}
