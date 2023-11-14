/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.NoSearchContextWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.OpenSearchIndexPartitionCreationSupplier;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.PitWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.ScrollWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.SearchWorker;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.ClusterClientFactory;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.SearchAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class OpenSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchService.class);

    static final Duration EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(30);

    private final SearchAccessor searchAccessor;
    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;
    private final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator;
    private final Buffer<Record<Event>> buffer;
    private final OpenSearchIndexPartitionCreationSupplier openSearchIndexPartitionCreationSupplier;
    private final ScheduledExecutorService scheduledExecutorService;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics;

    private SearchWorker searchWorker;
    private ScheduledFuture<?> searchWorkerFuture;

    public static OpenSearchService createOpenSearchService(final SearchAccessor searchAccessor,
                                                            final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                                                            final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                            final Buffer<Record<Event>> buffer,
                                                            final AcknowledgementSetManager acknowledgementSetManager,
                                                            final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics) {
        return new OpenSearchService(
                searchAccessor, sourceCoordinator, openSearchSourceConfiguration, buffer, Executors.newSingleThreadScheduledExecutor(),
                BufferAccumulator.create(buffer, openSearchSourceConfiguration.getSearchConfiguration().getBatchSize(), BUFFER_TIMEOUT),
                acknowledgementSetManager, openSearchSourcePluginMetrics);
    }

    private OpenSearchService(final SearchAccessor searchAccessor,
                              final SourceCoordinator<OpenSearchIndexProgressState> sourceCoordinator,
                              final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                              final Buffer<Record<Event>> buffer,
                              final ScheduledExecutorService scheduledExecutorService,
                              final BufferAccumulator<Record<Event>> bufferAccumulator,
                              final AcknowledgementSetManager acknowledgementSetManager,
                              final OpenSearchSourcePluginMetrics openSearchSourcePluginMetrics) {
        this.searchAccessor = searchAccessor;
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        this.sourceCoordinator = sourceCoordinator;
        this.sourceCoordinator.initialize();
        this.openSearchIndexPartitionCreationSupplier = new OpenSearchIndexPartitionCreationSupplier(openSearchSourceConfiguration, (ClusterClientFactory) searchAccessor);
        this.scheduledExecutorService = scheduledExecutorService;
        this.bufferAccumulator = bufferAccumulator;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.openSearchSourcePluginMetrics = openSearchSourcePluginMetrics;
    }

    public void start() {
        switch(searchAccessor.getSearchContextType()) {
            case POINT_IN_TIME:
                searchWorker = new PitWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier, acknowledgementSetManager, openSearchSourcePluginMetrics);
                break;
            case SCROLL:
                searchWorker = new ScrollWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier, acknowledgementSetManager, openSearchSourcePluginMetrics);
                break;
            case NONE:
                searchWorker = new NoSearchContextWorker(searchAccessor, openSearchSourceConfiguration, sourceCoordinator, bufferAccumulator, openSearchIndexPartitionCreationSupplier, acknowledgementSetManager, openSearchSourcePluginMetrics);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Search context type must be POINT_IN_TIME or SCROLL, type %s was given instead",
                                searchAccessor.getSearchContextType()));
        }

        final Instant startTime = openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime();
        final long waitTimeBeforeStartMillis = startTime.toEpochMilli() - Instant.now().toEpochMilli() < 0 ? 0L :
                startTime.toEpochMilli() - Instant.now().toEpochMilli();

        LOG.info("The opensearch source will start processing data at {}. It is currently {}", startTime, Instant.now());

        searchWorkerFuture = scheduledExecutorService.schedule(() -> searchWorker.run(), waitTimeBeforeStartMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdown();
        try {
            searchWorkerFuture.cancel(true);
            if (scheduledExecutorService.awaitTermination(EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT.getSeconds(), TimeUnit.SECONDS)) {
                LOG.info("Successfully waited for the search worker to terminate");
            } else {
                LOG.warn("Search worker did not terminate in time, forcing termination");
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for the search worker to terminate", e);
            scheduledExecutorService.shutdownNow();
        }
    }
}
