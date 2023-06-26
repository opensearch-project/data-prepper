/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException;
import org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreatePointInTimeResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeletePointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchPointInTimeRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

/**
 * PitWorker polls the source cluster via Point-In-Time contexts.
 */
public class PitWorker implements SearchWorker, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(PitWorker.class);

    private static final int STANDARD_BACKOFF_MILLIS = 30_000;
    private static final Duration BACKOFF_ON_PIT_LIMIT_REACHED = Duration.ofSeconds(60);

    static final String STARTING_KEEP_ALIVE = "15m";
    private static final Duration STARTING_KEEP_ALIVE_DURATION = Duration.ofMinutes(15);

    static final String EXTEND_KEEP_ALIVE_TIME = "1m";
    private static final Duration EXTEND_KEEP_ALIVE_DURATION = Duration.ofMinutes(1);

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    public PitWorker(final SearchAccessor searchAccessor,
                     final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                     final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                     final BufferAccumulator<Record<Event>> bufferAccumulator,
                     final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier) {
        this.searchAccessor = searchAccessor;
        this.sourceCoordinator = sourceCoordinator;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.bufferAccumulator = bufferAccumulator;
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
                    .withKeepAlive(STARTING_KEEP_ALIVE)
                    .build());

            LOG.debug("Created point in time for index {} with pit id {}", indexName, createPointInTimeResponse.getPitId());

            openSearchIndexProgressState.setPitId(createPointInTimeResponse.getPitId());
            openSearchIndexProgressState.setPitCreationTime(createPointInTimeResponse.getPitCreationTime());
            openSearchIndexProgressState.setKeepAlive(STARTING_KEEP_ALIVE_DURATION.toMillis());
            openSearchIndexProgressState.setSearchAfter(null);
        }

        final SearchConfiguration searchConfiguration = openSearchSourceConfiguration.getSearchConfiguration();
        SearchWithSearchAfterResults searchWithSearchAfterResults = null;

        // todo: Pass query and sort options from SearchConfiguration to the search request
        do {
            try {
                searchWithSearchAfterResults = searchAccessor.searchWithPit(SearchPointInTimeRequest.builder()
                        .withPitId(openSearchIndexProgressState.getPitId())
                        .withKeepAlive(EXTEND_KEEP_ALIVE_TIME)
                        .withPaginationSize(searchConfiguration.getBatchSize())
                        .withSearchAfter(getSearchAfter(openSearchIndexProgressState, searchWithSearchAfterResults))
                        .build());

                searchWithSearchAfterResults.getDocuments().stream().map(Record::new).forEach(record -> {
                    try {
                        bufferAccumulator.add(record);
                    } catch (Exception e) {
                        LOG.error("Failed writing OpenSearch documents to buffer. The last document created has document id '{}' from index '{}' : {}",
                                record.getData().getMetadata().getAttribute(DOCUMENT_ID_METADATA_ATTRIBUTE_NAME),
                                record.getData().getMetadata().getAttribute(INDEX_METADATA_ATTRIBUTE_NAME), e.getMessage());
                    }
                });
            } catch (final Exception e) {
                LOG.error("Received an exception while searching with PIT for index '{}'", indexName);
                throw new RuntimeException(e);
            }

            openSearchIndexProgressState.setSearchAfter(searchWithSearchAfterResults.getNextSearchAfter());
            openSearchIndexProgressState.setKeepAlive(Duration.ofMillis(openSearchIndexProgressState.getKeepAlive()).plus(EXTEND_KEEP_ALIVE_DURATION).toMillis());
            sourceCoordinator.saveProgressStateForPartition(indexName, openSearchIndexProgressState);
        } while (searchWithSearchAfterResults.getDocuments().size() == searchConfiguration.getBatchSize());

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            LOG.error("Failed flushing remaining OpenSearch documents to buffer due to: {}", e.getMessage());
        }

        // todo: This API call is failing with sigv4 enabled due to a mismatch in the signature. Tracking issue (https://github.com/opensearch-project/opensearch-java/issues/521)
        searchAccessor.deletePit(DeletePointInTimeRequest.builder().withPitId(openSearchIndexProgressState.getPitId()).build());
    }

    private OpenSearchIndexProgressState initializeProgressState() {
        return new OpenSearchIndexProgressState();
    }

    private List<String> getSearchAfter(final OpenSearchIndexProgressState openSearchIndexProgressState, final SearchWithSearchAfterResults searchWithSearchAfterResults) {
        if (Objects.isNull(searchWithSearchAfterResults)) {
            if (Objects.isNull(openSearchIndexProgressState.getSearchAfter())) {
                return null;
            } else {
                return openSearchIndexProgressState.getSearchAfter();
            }
        }

        return searchWithSearchAfterResults.getNextSearchAfter();
    }
}
