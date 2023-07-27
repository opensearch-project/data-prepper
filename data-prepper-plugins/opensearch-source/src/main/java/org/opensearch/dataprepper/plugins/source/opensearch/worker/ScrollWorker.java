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
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
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

import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.DOCUMENT_ID_METADATA_ATTRIBUTE_NAME;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.MetadataKeyAttributes.INDEX_METADATA_ATTRIBUTE_NAME;

/**
 * ScrollWorker polls the source cluster via scroll contexts.
 */
public class ScrollWorker implements SearchWorker {

    private static final Logger LOG = LoggerFactory.getLogger(ScrollWorker.class);
    private static final int STANDARD_BACKOFF_MILLIS = 30_000;
    private static final Duration BACKOFF_ON_SCROLL_LIMIT_REACHED = Duration.ofSeconds(120);
    static final String SCROLL_TIME_PER_BATCH = "1m";

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;

    public ScrollWorker(final SearchAccessor searchAccessor,
                        final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                        final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                        final BufferAccumulator<Record<Event>> bufferAccumulator,
                        final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier) {
        this.searchAccessor = searchAccessor;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.sourceCoordinator = sourceCoordinator;
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
                LOG.warn("ScrollWorker received an exception from the source coordinator. There is a potential for duplicate data for index {}, giving up partition and getting next partition: {}", indexPartition.get().getPartitionKey(), e.getMessage());
                sourceCoordinator.giveUpPartitions();
            } catch (final SearchContextLimitException e) {
                LOG.warn("Received SearchContextLimitExceeded exception for index {}. Giving up index and waiting {} seconds before retrying: {}",
                        indexPartition.get().getPartitionKey(), BACKOFF_ON_SCROLL_LIMIT_REACHED.getSeconds(), e.getMessage());
                sourceCoordinator.giveUpPartitions();
                try {
                    Thread.sleep(BACKOFF_ON_SCROLL_LIMIT_REACHED.toMillis());
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

        final Integer batchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();

        final CreateScrollResponse createScrollResponse = searchAccessor.createScroll(CreateScrollRequest.builder()
                .withScrollTime(SCROLL_TIME_PER_BATCH)
                .withSize(openSearchSourceConfiguration.getSearchConfiguration().getBatchSize())
                .withIndex(indexName)
                .build());

        writeDocumentsToBuffer(createScrollResponse.getDocuments());

        SearchScrollResponse searchScrollResponse = null;

        if (createScrollResponse.getDocuments().size() == batchSize) {
            do {
                try {
                    searchScrollResponse = searchAccessor.searchWithScroll(SearchScrollRequest.builder()
                            .withScrollId(Objects.nonNull(searchScrollResponse) && Objects.nonNull(searchScrollResponse.getScrollId()) ? searchScrollResponse.getScrollId() : createScrollResponse.getScrollId())
                            .withScrollTime(SCROLL_TIME_PER_BATCH)
                            .build());

                    writeDocumentsToBuffer(searchScrollResponse.getDocuments());
                    sourceCoordinator.saveProgressStateForPartition(indexName, null);
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
            LOG.error("Failed flushing remaining OpenSearch documents to buffer due to: {}", e.getMessage());
        }
    }

    private void writeDocumentsToBuffer(final List<Event> documents) {
        documents.stream().map(Record::new).forEach(record -> {
            try {
                bufferAccumulator.add(record);
            } catch (Exception e) {
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
