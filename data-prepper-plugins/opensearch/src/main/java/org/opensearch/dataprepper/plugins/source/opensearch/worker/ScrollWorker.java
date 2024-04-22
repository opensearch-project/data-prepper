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
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.IndexNotFoundException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.exceptions.SearchContextLimitException;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.CreateScrollResponse;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.DeleteScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollRequest;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchScrollResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

/**
 * ScrollWorker polls the source cluster via scroll contexts.
 */
public class ScrollWorker implements SearchWorker {

    private static final Logger LOG = LoggerFactory.getLogger(ScrollWorker.class);
    private static final Duration BACKOFF_ON_SCROLL_LIMIT_REACHED = Duration.ofSeconds(120);
    static final String SCROLL_TIME_PER_BATCH = "1m";

    private final ObjectMapper objectMapper;
    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    private int noAvailableIndicesCount = 0;

    public ScrollWorker(final ObjectMapper objectMapper,
                        final SearchAccessor searchAccessor,
                        final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                        final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                        final BufferAccumulator<Record<Event>> bufferAccumulator,
                        final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics) {
        this.objectMapper = objectMapper;
        this.searchAccessor = searchAccessor;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.sourceCoordinator = sourceCoordinator;
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
                        LOG.info("The ScrollWorker was interrupted while sleeping after acquiring no indices to process, stopping processing");
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
                } catch (final PartitionUpdateException | PartitionNotFoundException | PartitionNotOwnedException e) {
                    LOG.warn("ScrollWorker received an exception from the source coordinator. There is a potential for duplicate data for index {}, giving up partition and getting next partition: {}", indexPartition.get().getPartitionKey(), e.getMessage());
                    sourceCoordinator.giveUpPartition(indexPartition.get());
                } catch (final SearchContextLimitException e) {
                    LOG.warn("Received SearchContextLimitExceeded exception for index {}. Giving up index and waiting {} seconds before retrying: {}",
                            indexPartition.get().getPartitionKey(), BACKOFF_ON_SCROLL_LIMIT_REACHED.getSeconds(), e.getMessage());
                    sourceCoordinator.giveUpPartition(indexPartition.get());
                    openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                    try {
                        Thread.sleep(BACKOFF_ON_SCROLL_LIMIT_REACHED.toMillis());
                    } catch (final InterruptedException ex) {
                        return;
                    }
                } catch (final IndexNotFoundException e){
                    LOG.warn("{}, marking index as complete and continuing processing", e.getMessage());
                    sourceCoordinator.completePartition(indexPartition.get().getPartitionKey(), false);
                } catch (final RuntimeException e) {
                    LOG.error("Unknown exception while processing index '{}':", indexPartition.get().getPartitionKey(), e);
                    sourceCoordinator.giveUpPartition(indexPartition.get());
                    openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while trying to get index to process with scroll, backing off and retrying", e);
                openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
                try {
                    Thread.sleep(BACKOFF_ON_EXCEPTION.toMillis());
                } catch (final InterruptedException ex) {
                    LOG.info("The ScrollWorker was interrupted before backing off and retrying, stopping processing");
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

        final Integer batchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();

        final CreateScrollResponse createScrollResponse = searchAccessor.createScroll(CreateScrollRequest.builder()
                .withScrollTime(SCROLL_TIME_PER_BATCH)
                .withSize(openSearchSourceConfiguration.getSearchConfiguration().getBatchSize())
                .withIndex(indexName)
                .build());

        writeDocumentsToBuffer(createScrollResponse.getDocuments(), acknowledgementSet);

        SearchScrollResponse searchScrollResponse = null;

        if (createScrollResponse.getDocuments().size() == batchSize) {
            do {
                try {
                    searchScrollResponse = searchAccessor.searchWithScroll(SearchScrollRequest.builder()
                            .withScrollId(Objects.nonNull(searchScrollResponse) && Objects.nonNull(searchScrollResponse.getScrollId()) ? searchScrollResponse.getScrollId() : createScrollResponse.getScrollId())
                            .withScrollTime(SCROLL_TIME_PER_BATCH)
                            .build());

                    writeDocumentsToBuffer(searchScrollResponse.getDocuments(), acknowledgementSet);

                    if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                        LOG.debug("Renew ownership of index {}", indexName);
                        sourceCoordinator.saveProgressStateForPartition(indexName, null);
                        lastCheckpointTime = System.currentTimeMillis();
                    }
                } catch (final Exception e) {
                    deleteScroll(createScrollResponse.getScrollId());
                    throw e;
                }
            } while (searchScrollResponse.getDocuments().size() == batchSize);
        }

        deleteScroll(createScrollResponse.getScrollId());

        try {
            bufferAccumulator.flush();
        } catch (final Exception e) {
            openSearchSourcePluginMetrics.getProcessingErrorsCounter().increment();
            LOG.error("Failed flushing remaining OpenSearch documents to buffer due to: {}", e.getMessage());
        }
    }

    private void writeDocumentsToBuffer(final List<Event> documents,
                                        final AcknowledgementSet acknowledgementSet) {
        documents.stream().map(Record::new).forEach(record -> {
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
    }

    // todo: This API call is failing with sigv4 enabled due to a mismatch in the signature. Tracking issue (https://github.com/opensearch-project/opensearch-java/issues/521)
    private void deleteScroll(final String scrollId) {
        searchAccessor.deleteScroll(DeleteScrollRequest.builder()
                .withScrollId(scrollId)
                .build());
    }
}
