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
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
    static final String DEFAULT_KEEP_ALIVE = "30m"; // 30 minutes (will be extended during search)
    private static final Duration BACKOFF_ON_PIT_LIMIT_REACHED = Duration.ofSeconds(60);

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
                } catch (final InterruptedException e) {
                    LOG.info("The PitWorker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                    return;
                }
            }

            try {
                processIndex(indexPartition.get());

                sourceCoordinator.closePartition(
                        indexPartition.get().getPartitionKey(),
                        openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(),
                        openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount());
            } catch (final PartitionUpdateException | PartitionNotFoundException | PartitionNotOwnedException e) {
                LOG.warn("PitWorker received an exception from the source coordinator. There is a potential for duplicate data for index {}, giving up partition and getting next partition: {}", indexPartition.get().getPartitionKey(), e.getMessage());
                sourceCoordinator.giveUpPartitions();
            } catch (final SearchContextLimitException e) {
                LOG.warn("Received SearchContextLimitExceeded exception for index {}. Giving up index and waiting {} seconds before retrying: {}",
                        indexPartition.get().getPartitionKey(), BACKOFF_ON_PIT_LIMIT_REACHED.getSeconds(), e.getMessage());
                sourceCoordinator.giveUpPartitions();
                try {
                    Thread.sleep(BACKOFF_ON_PIT_LIMIT_REACHED.toMillis());
                } catch (final InterruptedException ex) {
                    return;
                }
            } catch (final RuntimeException e) {
                LOG.error("Unknown exception while processing index '{}':", indexPartition.get().getPartitionKey(), e);
                sourceCoordinator.giveUpPartitions();
            }
        }
    }

    private void processIndex(final SourcePartition<OpenSearchIndexProgressState> openSearchIndexPartition) {
        final String indexName = openSearchIndexPartition.getPartitionKey();
        Optional<OpenSearchIndexProgressState> openSearchIndexProgressStateOptional = openSearchIndexPartition.getPartitionState();

        if (openSearchIndexProgressStateOptional.isEmpty()) {
            openSearchIndexProgressStateOptional = Optional.of(initializeProgressState());
        }

        final OpenSearchIndexProgressState openSearchIndexProgressState = openSearchIndexProgressStateOptional.get();

        if (!openSearchIndexProgressState.hasValidPointInTime()) {
            final CreatePointInTimeResponse createPointInTimeResponse = searchAccessor.createPit(CreatePointInTimeRequest.builder()
                    .withIndex(indexName)
                    .withKeepAlive(DEFAULT_KEEP_ALIVE)
                    .build());

            LOG.debug("Created point in time for index {} with pit id {}", indexName, createPointInTimeResponse.getPitId());

            openSearchIndexProgressState.setPitId(createPointInTimeResponse.getPitId());
            openSearchIndexProgressState.setPitCreationTime(createPointInTimeResponse.getPitCreationTime());
            openSearchIndexProgressState.setKeepAlive(createPointInTimeResponse.getKeepAlive());
        }

        // todo: Implement search with pit and write documents to buffer, checkpoint with calls to saveState


        // todo: This API call is failing with sigv4 enabled due to a mismatch in the signature
        searchAccessor.deletePit(DeletePointInTimeRequest.builder().withPitId(openSearchIndexProgressState.getPitId()).build());
    }

    private OpenSearchIndexProgressState initializeProgressState() {
        return new OpenSearchIndexProgressState();
    }
}
