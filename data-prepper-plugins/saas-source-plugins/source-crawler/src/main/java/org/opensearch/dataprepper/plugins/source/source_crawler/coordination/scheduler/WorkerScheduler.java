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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.SaasWorkerProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

/**
 * Worker class for executing the partitioned work created while crawling a source.
 * Each SAAS source will provide their own specific source extraction logic.
 */
public class WorkerScheduler implements Runnable {

    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofSeconds(20);
    private static final Logger log = LoggerFactory.getLogger(WorkerScheduler.class);
    private static final int RETRY_BACKOFF_ON_EXCEPTION_MILLIS = 5_000;
    private static final Duration DEFAULT_SLEEP_DURATION_MILLIS = Duration.ofMillis(10000);
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
    }

    @Override
    public void run() {
        log.info("Worker thread started");
        log.info("Processing Partitions");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Get the next available partition from the coordinator
                Optional<EnhancedSourcePartition> partition =
                        sourceCoordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE);
                if (partition.isPresent()) {
                    // Process the partition (source extraction logic)
                    processPartition(partition.get(), buffer);

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
                log.error("Error processing partition", e);
                try {
                    Thread.sleep(RETRY_BACKOFF_ON_EXCEPTION_MILLIS);
                } catch (InterruptedException ex) {
                    log.warn("Thread interrupted while waiting to retry", ex);
                }
            }
        }
        log.warn("SourceItemWorker Scheduler is interrupted, looks like shutdown has triggered");
    }

    private void processPartition(EnhancedSourcePartition partition, Buffer<Record<Event>> buffer) {
        // Implement your source extraction logic here
        // Update the partition state or commit the partition as needed
        // Commit the partition to mark it as processed
        if (partition.getProgressState().isPresent()) {
            AcknowledgementSet acknowledgementSet = null;
            if (sourceConfig.isAcknowledgments()) {
                acknowledgementSet = createAcknowledgementSet(partition);
            }
            crawler.executePartition((SaasWorkerProgressState) partition.getProgressState().get(), buffer, acknowledgementSet);
        }
        sourceCoordinator.completePartition(partition);
    }

    private AcknowledgementSet createAcknowledgementSet(EnhancedSourcePartition partition) {
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
