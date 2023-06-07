/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * PitWorker polls the source cluster via Point-In-Time contexts.
 */
public class PitWorker implements SearchWorker, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(PitWorker.class);

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final Buffer<Record<Event>> buffer;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;

    public PitWorker(final SearchAccessor searchAccessor,
                     final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                     final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                     final Buffer<Record<Event>> buffer,
                     final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier) {
        this.searchAccessor = searchAccessor;
        this.sourceCoordinator = sourceCoordinator;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        this.openSearchIndexPartitionCreationSupplier = openSearchIndexPartitionCreationSupplier;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            final Optional<SourcePartition<OpenSearchIndexProgressState>> indexPartition = sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier);

            if (indexPartition.isEmpty()) {
                try {
                    Thread.sleep(STANDARD_BACKOFF_MILLIS);
                    continue;
                } catch (InterruptedException e) {
                    LOG.info("The PitWorker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                    return;
                }
            }

            // todo: process the index and write to the buffer

            sourceCoordinator.closePartition(
                    indexPartition.get().getPartitionKey(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(),
                    openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount());
        }
    }
}
