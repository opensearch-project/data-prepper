package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Worker class for executing the partitioned work created while crawling a source.
 * Each SAAS source will provide their own specific source extraction logic.
 */
public class SourceItemWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SourceItemWorker.class);
    private static final int RETRY_BACKOFF_ON_EXCEPTION_MILLIS = 5_000;

    private final EnhancedSourceCoordinator sourceCoordinator;

    private final SaasSourceConfig sourceConfig;

    public SourceItemWorker(Buffer<Record<Event>> buffer,
                            EnhancedSourceCoordinator sourceCoordinator,
                            SaasSourceConfig sourceConfig) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void run() {
        log.info("Worker thread started");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Get the next available partition from the coordinator
                Optional<EnhancedSourcePartition> partition = sourceCoordinator.acquireAvailablePartition(SaasSourcePartition.PARTITION_TYPE);
                if (partition.isPresent()) {
                    // Process the partition (source extraction logic)
                    processPartition(partition.get());

                } else {
                    log.info("No partition available. Going to Sleep for a while ");
                    try {
                        Thread.sleep(10000);
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

    private void processPartition(EnhancedSourcePartition partition) {
        log.info("Processing partition: {}", partition.getPartitionKey());
        // Implement your source extraction logic here
        // Update the partition state or commit the partition as needed
        // Commit the partition to mark it as processed
        sourceCoordinator.completePartition(partition);
    }
}
