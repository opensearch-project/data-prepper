/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
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
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.IndexNotFoundException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.NoSearchContextSearchRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.BACKOFF_ON_EXCEPTION;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.DEFAULT_CHECKPOINT_INTERVAL_MILLS;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.calculateExponentialBackoffAndJitter;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.completeIndexPartition;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.createAcknowledgmentSet;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

public class NoSearchContextWorker implements SearchWorker, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(NoSearchContextWorker.class);

    private final ObjectMapper objectMapper;
    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    private int noAvailableIndicesCount = 0;

    public NoSearchContextWorker(final ObjectMapper objectMapper,
                                 final SearchAccessor searchAccessor,
                                 final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                 final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                                 final BufferAccumulator<Record<Event>> bufferAccumulator,
                                 final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier,
                                 final AcknowledgementSetManager acknowledgementSetManager,
                                 final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics) {
        this.objectMapper = objectMapper;
        this.searchAccessor = searchAccessor;
        this.sourceCoordinator = sourceCoordinator;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.bufferAccumulator = bufferAccumulator;
        this.openSearchIndexPartitionCreationSupplier = openSearchIndexPartitionCreationSupplier;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.openSearchSourcePluginMetrics = openSearchSourcePluginMetrics;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {

            try {

                final Optional<SourcePartition<OpenSearchIndexProgressState>> indexPartition = sourceCoordinator.getNextPartition(openSearchIndexPartitionCreationSupplier);

                if (indexPartition.isEmpty()) {
                    try {
                        Thread.sleep(calculateExponentialBackoffAndJitter(++noAvailableIndicesCount));
                        continue;
                    } catch (final InterruptedException e) {
                        LOG.info("The search_after worker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                        return;
                    }
                }

                noAvailableIndicesCount = 0;

                try {
                    final AcknowledgementSet acknowledgementSet = createAcknowledgmentSet(
                            acknowledgementSetManager,
                            openSearchSourceConfiguration,
                            sourceCoordinator,
                            indexPartition.get());

                    openSearchSourcePluginMetrics.getIndexProcessingTimeTimer().record(() -> processIndex(indexPartition.get(), acknowledgementSet));

                    completeIndexPartition(openSearchSourceConfiguration, acknowledgementSet,
                            indexPartition.get(), sourceCoordinator);

                    openSearchSourcePluginMetrics.getIndicesProcessedCounter().increment();
                    LOG.info("Completed processing for index: '{}'", indexPartition.get().getPartitionKey());
                } catch (final PartitionUpdateException | PartitionNotFoundException | PartitionNotOwnedException e) {
                    LOG.warn("The search_after worker received an exception from the source coordinator. There is a potential for duplicate data for index {}, giving up partition and getting next partition: {}", indexPartition.get().getPartitionKey(), e.getMessage());
                    sourceCoordinator.giveUpPartitions();
                } catch (final IndexNotFoundException e) {
                    LOG.warn("{}, marking index as complete and continuing processing", e.getMessage());
                    sourceCoordinator.completePartition(indexPartition.get().getPartitionKey(), false);
                } catch (final Exception e) {
                    LOG.error("Unknown exception while processing index '{}', moving on to another index:", indexPartition.get().getPartitionKey(), e);
                    sourceCoordinator.giveUpPartitions();
                    openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                }
            } catch (final Exception e) {
                openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                LOG.error("Received an exception while trying to get index to process with search_after, backing off and retrying", e);
                try {
                    Thread.sleep(BACKOFF_ON_EXCEPTION.toMillis());
                } catch (final InterruptedException ex) {
                    LOG.info("The search_after worker was interrupted before backing off and retrying, stopping processing");
                    return;
                }
            }
        }
    }

    private void processIndex(final SourcePartition<OpenSearchIndexProgressState> openSearchIndexPartition,
                              final AcknowledgementSet acknowledgementSet) {
        final String indexName = openSearchIndexPartition.getPartitionKey();
        long lastCheckpointTime = System.currentTimeMillis();

        LOG.info("Started processing for index: '{}'", indexName);

        Optional<OpenSearchIndexProgressState> openSearchIndexProgressStateOptional = openSearchIndexPartition.getPartitionState();

        if (openSearchIndexProgressStateOptional.isEmpty()) {
            openSearchIndexProgressStateOptional = Optional.of(initializeProgressState());
        }

        final OpenSearchIndexProgressState openSearchIndexProgressState = openSearchIndexProgressStateOptional.get();

        final SearchConfiguration searchConfiguration = openSearchSourceConfiguration.getSearchConfiguration();
        SearchWithSearchAfterResults searchWithSearchAfterResults = null;

        // todo: Pass query and sort options from SearchConfiguration to the search request
        do {
            searchWithSearchAfterResults = searchAccessor.searchWithoutSearchContext(NoSearchContextSearchRequest.builder()
                            .withIndex(indexName)
                            .withPaginationSize(searchConfiguration.getBatchSize())
                            .withSearchAfter(getSearchAfter(openSearchIndexProgressState, searchWithSearchAfterResults))
                            .build());

            searchWithSearchAfterResults.getDocuments().stream().map(Record::new).forEach(record -> {
                try {
                    final long documentBytes = objectMapper.writeValueAsBytes(record.getData().getJsonNode()).length;
                    openSearchSourcePluginMetrics.getBytesReceivedSummary().record(documentBytes);
                    if (Objects.nonNull(acknowledgementSet)) {
                        acknowledgementSet.add(record.getData());
                    }
                    bufferAccumulator.add(record);
                    openSearchSourcePluginMetrics.getDocumentsProcessedCounter().increment();
                    openSearchSourcePluginMetrics.getBytesProcessedSummary().record(documentBytes);
                } catch (Exception e) {
                    openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                    LOG.error("Failed writing OpenSearch documents to buffer. The last document created has document id '{}' from index '{}' : {}",
                            record.getData().getMetadata().getAttribute(DOCUMENT_ID_METADATA_ATTRIBUTE_NAME),
                            record.getData().getMetadata().getAttribute(INDEX_METADATA_ATTRIBUTE_NAME), e.getMessage());
                }
            });

            openSearchIndexProgressState.setSearchAfter(searchWithSearchAfterResults.getNextSearchAfter());

            if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                LOG.debug("Renew ownership of index {}", indexName);
                sourceCoordinator.saveProgressStateForPartition(indexName, openSearchIndexProgressState);
                lastCheckpointTime = System.currentTimeMillis();
            }
        } while (searchWithSearchAfterResults.getDocuments().size() == searchConfiguration.getBatchSize());

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
            LOG.error("Failed writing remaining OpenSearch documents to buffer due to: {}", e.getMessage());
        }
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
