/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus;
import org.opensearch.dataprepper.plugins.mongo.model.StreamLoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_PREFIX;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.STREAM_PREFIX;

public class ExportWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportWorker.class);
    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    static final String SUCCESS_PARTITION_COUNTER_NAME = "exportPartitionSuccessTotal";
    static final String FAILURE_PARTITION_COUNTER_NAME = "exportPartitionFailureTotal";
    static final String ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE = "activeExportPartitionConsumers";

    /**
     * Maximum concurrent data loader per node
     */
    private static final int MAX_JOB_COUNT = 1;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2_000;

    private static final String S3_PATH_DELIMITER = "/";

    /**
     * Start Line is the checkpoint
     */
    private final int startLine;

    private final AcknowledgementSetManager acknowledgementSetManager;
    private final MongoDBSourceConfig sourceConfig;
    private final Counter successPartitionCounter;
    private final Counter failureParitionCounter;
    private final AtomicInteger activeExportPartitionConsumerGauge;

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 10;

    private final RecordBufferWriter recordBufferWriter;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final ExecutorService executor;
    private final PluginMetrics pluginMetrics;


    public ExportWorker(final EnhancedSourceCoordinator sourceCoordinator,
                        final Buffer<Record<Event>> buffer,
                        final PluginMetrics pluginMetrics,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final MongoDBSourceConfig sourceConfig) {
        this.sourceCoordinator = sourceCoordinator;
        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator, pluginMetrics);
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
        this.startLine = 0;// replace it with checkpoint line
        this.pluginMetrics = pluginMetrics;
        this.successPartitionCounter = pluginMetrics.counter(SUCCESS_PARTITION_COUNTER_NAME);
        this.failureParitionCounter = pluginMetrics.counter(FAILURE_PARTITION_COUNTER_NAME);
        this.activeExportPartitionConsumerGauge = pluginMetrics.gauge(ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE, numOfWorkers);
    }

    @Override
    public void run() {
        LOG.info("Starting Export worker to process partitions for export");

        while (!Thread.currentThread().isInterrupted()) {
            DataQueryPartition dataQueryPartition = null;
            try {
                if (numOfWorkers.get() < MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE);

                    if (sourcePartition.isPresent()) {
                        dataQueryPartition = (DataQueryPartition) sourcePartition.get();
                        final AcknowledgementSet acknowledgementSet = createAcknowledgementSet(dataQueryPartition).orElse(null);
                        final String s3PathPrefix;
                        if (sourceCoordinator.getPartitionPrefix() != null ) {
                            s3PathPrefix = sourceConfig.getS3Prefix() + S3_PATH_DELIMITER + sourceCoordinator.getPartitionPrefix();
                        } else {
                            s3PathPrefix = sourceConfig.getS3Prefix();
                        }
                        final DataQueryPartitionCheckpoint partitionCheckpoint =  new DataQueryPartitionCheckpoint(sourceCoordinator, dataQueryPartition);
                        final PartitionKeyRecordConverter recordConverter = new PartitionKeyRecordConverter(dataQueryPartition.getCollection(),
                                ExportPartition.PARTITION_TYPE, s3PathPrefix);
                        final ExportPartitionWorker exportPartitionWorker = new ExportPartitionWorker(recordBufferWriter, recordConverter,
                                dataQueryPartition, acknowledgementSet, sourceConfig, partitionCheckpoint, Instant.now().toEpochMilli(), pluginMetrics);
                        final CompletableFuture<Void> runLoader = CompletableFuture.runAsync(exportPartitionWorker, executor);
                        runLoader.whenComplete(completePartitionLoader(dataQueryPartition));
                        numOfWorkers.incrementAndGet();
                        activeExportPartitionConsumerGauge.incrementAndGet();
                    }
                }
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ExportWorker was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing an export data partition, backing off and retrying", e);
                if (dataQueryPartition != null) {
                    sourceCoordinator.giveUpPartition(dataQueryPartition);
                }

                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The ExportWorker was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("ExportWorker is interrupted, stopping all data partition loaders...");
        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        executor.shutdown();
        ExportPartitionWorker.stopAll();
    }

    private Optional<AcknowledgementSet> createAcknowledgementSet(final DataQueryPartition partition) {
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            return Optional.of(acknowledgementSetManager.create((result) -> {
                if (result) {
                    completeDataLoader(partition).accept(null, null);
                    LOG.info("Received acknowledgment of completion from sink for data query {}", partition.getPartitionKey());
                } else {
                    LOG.warn("Negative acknowledgment received for data query {}, retrying", partition.getPartitionKey());
                    sourceCoordinator.giveUpPartition(partition);
                }
            }, sourceConfig.getPartitionAcknowledgmentTimeout()));
        }
        return Optional.empty();
    }

    private BiConsumer<Void, Throwable> completeDataLoader(final DataQueryPartition dataQueryPartition) {
        return (v, ex) -> {
            if (!sourceConfig.isAcknowledgmentsEnabled()) {
                numOfWorkers.decrementAndGet();
                activeExportPartitionConsumerGauge.decrementAndGet();
            }
            if (ex == null) {
                successPartitionCounter.increment();
                // Update global state
                updateState(dataQueryPartition.getCollection(),
                    dataQueryPartition.getProgressState().get().getLoadedRecords());
                // After global state is updated, mask the partition as completed.
                sourceCoordinator.completePartition(dataQueryPartition);

            } else {
                giveUpPartition(dataQueryPartition, ex);
            }
        };
    }

    private void giveUpPartition(final DataQueryPartition dataQueryPartition, final Throwable ex) {
        // The data loader must have already done one last checkpointing.
        LOG.error("Loading Data Query partition completed with an exception.", ex);
        failureParitionCounter.increment();
        // Release the ownership
        sourceCoordinator.giveUpPartition(dataQueryPartition);
    }

    private BiConsumer<Void, Throwable> completePartitionLoader(final DataQueryPartition dataQueryPartition) {
        if (!sourceConfig.isAcknowledgmentsEnabled()) {
            return completeDataLoader(dataQueryPartition);
        } else {
            return (v, ex) -> {
                if (ex != null) {
                    giveUpPartition(dataQueryPartition, ex);
                }
                numOfWorkers.decrementAndGet();
                activeExportPartitionConsumerGauge.decrementAndGet();
            };
        }
    }

    /**
     * <p>There is a global state with sourcePartitionKey the export Arn,
     * to track the number of files are processed. </p>
     * <p>Each time, load of a data file is completed,
     * The state must be updated.</p>
     * Note that the state may be updated since multiple threads are updating the same state.
     * Retry is required.
     *
     * @param collection collection name and database.
     * @param loaded    Number records Loaded.
     */
    private void updateState(final String collection, final long loaded) {

        final String exportPartitionKey = EXPORT_PREFIX + collection;

        // Unlimited retries
        // The state be out of dated when updating.
        while (!Thread.currentThread().isInterrupted()) {
            Optional<EnhancedSourcePartition> globalPartition = sourceCoordinator.getPartition(exportPartitionKey);
            if (globalPartition.isEmpty()) {
                LOG.error("Failed to get load status for " + exportPartitionKey);
                // TODO add wait time
                continue;
            }

            final GlobalState globalState = (GlobalState) globalPartition.get();
            final ExportLoadStatus exportLoadStatus = ExportLoadStatus.fromMap(globalState.getProgressState().get());
            exportLoadStatus.setLoadedPartitions(exportLoadStatus.getLoadedPartitions() + 1);
            LOG.info("Current status: total {}, loaded {}", exportLoadStatus.getTotalPartitions(), exportLoadStatus.getLoadedPartitions());

            exportLoadStatus.setLoadedRecords(exportLoadStatus.getLoadedRecords() + loaded);
            exportLoadStatus.setLastUpdateTimestamp(Instant.now().toEpochMilli());
            globalState.setProgressState(exportLoadStatus.toMap());

            try {
                sourceCoordinator.saveProgressStateForPartition(globalState, null);
                // if all load are completed.
                if (exportLoadStatus.isTotalParitionsComplete() &&
                        exportLoadStatus.getLoadedPartitions() == exportLoadStatus.getTotalPartitions()) {
                    LOG.info("All Exports are done, streaming can continue...");
                    final StreamLoadStatus streamLoadStatus = new StreamLoadStatus(Instant.now().toEpochMilli());
                    sourceCoordinator.createPartition(new GlobalState(STREAM_PREFIX + collection, streamLoadStatus.toMap()));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of date, will retry..");
            }
        }
    }
}
