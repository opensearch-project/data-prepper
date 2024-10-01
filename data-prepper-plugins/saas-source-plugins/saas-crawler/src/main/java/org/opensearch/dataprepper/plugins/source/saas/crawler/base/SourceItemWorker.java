package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        log.info("Worker thread run method");
    }
}
