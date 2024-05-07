/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.PartitionIdentifier;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.DataQueryProgressState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus;
import org.opensearch.dataprepper.plugins.mongo.model.PartitionIdentifierBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class ExportScheduler implements Runnable {
    public static final String EXPORT_PREFIX = "EXPORT-";
    private static final Logger LOG = LoggerFactory.getLogger(ExportScheduler.class);
    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    private static final Duration DEFAULT_CLOSE_DURATION = Duration.ofMinutes(10);
    private static final int DEFAULT_MAX_CLOSE_COUNT = 36;
    private static final String COMPLETED_STATUS = "Completed";
    private static final String FAILED_STATUS = "Failed";
    static final int DEFAULT_GET_PARTITION_BACKOFF_MILLIS = 3000;
    static final String EXPORT_JOB_SUCCESS_COUNT = "exportJobSuccess";
    static final String EXPORT_JOB_FAILURE_COUNT = "exportJobFailure";
    static final String EXPORT_PARTITION_QUERY_TOTAL_COUNT = "exportPartitionQueryTotal";
    static final String EXPORT_RECORDS_TOTAL_COUNT = "exportRecordsTotal";
    private final PluginMetrics pluginMetrics;
    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final MongoDBExportPartitionSupplier mongoDBExportPartitionSupplier;
    private final Counter exportJobSuccessCounter;
    private final Counter exportJobFailureCounter;

    private final Counter exportPartitionTotalCounter;
    private final Counter exportRecordsTotalCounter;

    public ExportScheduler(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                           final MongoDBExportPartitionSupplier mongoDBExportPartitionSupplier,
                           final PluginMetrics pluginMetrics) {
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.mongoDBExportPartitionSupplier = mongoDBExportPartitionSupplier;
        this.pluginMetrics = pluginMetrics;

        exportJobSuccessCounter = pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT);
        exportJobFailureCounter = pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT);
        exportPartitionTotalCounter = pluginMetrics.counter(EXPORT_PARTITION_QUERY_TOTAL_COUNT);
        exportRecordsTotalCounter = pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT);
    }

    @Override
    public void run() {
        LOG.info("Start running Export Scheduler");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = enhancedSourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    final ExportPartition exportPartition = (ExportPartition) sourcePartition.get();
                    LOG.info("Acquired an export partition: {}", exportPartition.getPartitionKey());

                    final String exportPartitionKey = EXPORT_PREFIX + exportPartition.getCollection();
                    Optional<EnhancedSourcePartition> globalPartition = enhancedSourceCoordinator
                            .getPartition(exportPartitionKey);
                    while (globalPartition.isEmpty()) {
                        LOG.warn("Wait for global partition to be created.");
                        Thread.sleep(DEFAULT_GET_PARTITION_BACKOFF_MILLIS);
                        globalPartition = enhancedSourceCoordinator.getPartition(exportPartitionKey);
                    }

                    final PartitionIdentifierBatch partitionIdentifierBatch = mongoDBExportPartitionSupplier.apply(exportPartition);

                    createDataQueryPartitions(
                            exportPartition.getCollection(), Instant.now(), partitionIdentifierBatch.getPartitionIdentifiers(),
                            (GlobalState) globalPartition.get());
                    updateExportPartition(exportPartition, partitionIdentifierBatch);

                    if (partitionIdentifierBatch.isLastBatch()) {
                        completeExportPartition(exportPartition);
                        markTotalPartitionsAsComplete(exportPartition.getCollection());
                    }
                }
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception during export from DocumentDB, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("Export scheduler interrupted, looks like shutdown has triggered");
    }

    private boolean createDataQueryPartitions(final String collection,
                                           final Instant exportTime,
                                           final List<PartitionIdentifier> partitionIdentifiers,
                                              final GlobalState globalState) {
        AtomicLong totalQueries = new AtomicLong();
        partitionIdentifiers.forEach(partitionIdentifier -> {
            final DataQueryProgressState progressState = new DataQueryProgressState();
            progressState.setExecutedQueries(0);
            progressState.setLoadedRecords(0);
            progressState.setStartTime(exportTime.toEpochMilli());

            totalQueries.getAndIncrement();
            final DataQueryPartition partition = new DataQueryPartition(partitionIdentifier.getPartitionKey(), progressState);
            enhancedSourceCoordinator.createPartition(partition);
        });

        if (totalQueries.get() > 0) {
            exportPartitionTotalCounter.increment(totalQueries.get());
            final ExportLoadStatus exportLoadStatus = ExportLoadStatus.fromMap(globalState.getProgressState().get());
            totalQueries.getAndAdd(exportLoadStatus.getTotalPartitions());
            exportLoadStatus.setTotalPartitions(totalQueries.get());
            exportLoadStatus.setLastUpdateTimestamp(Instant.now().toEpochMilli());
            globalState.setProgressState(exportLoadStatus.toMap());

            // Currently, we need to maintain a global state to track the overall progress.
            // So that we can easily tell if all the export files are loaded
            enhancedSourceCoordinator.saveProgressStateForPartition(globalState, null);
            return true;
        } else {
            return false;
        }
    }

    private void updateExportPartition(final ExportPartition exportPartition,
                                       final PartitionIdentifierBatch partitionIdentifierBatch) {
        final ExportProgressState state = exportPartition.getProgressState().get();
        if (partitionIdentifierBatch.getEndDocId() != null) {
            state.setLastEndDocId(partitionIdentifierBatch.getEndDocId());
            enhancedSourceCoordinator.saveProgressStateForPartition(exportPartition, null);
        }
    }

    private void completeExportPartition(final ExportPartition exportPartition) {
        exportJobSuccessCounter.increment();
        final ExportProgressState state = exportPartition.getProgressState().get();
        state.setStatus(COMPLETED_STATUS);
        enhancedSourceCoordinator.completePartition(exportPartition);
    }

    private void markTotalPartitionsAsComplete(final String collection) {
        final String exportPartitionKey = EXPORT_PREFIX + collection;
        Optional<EnhancedSourcePartition> globalPartition = enhancedSourceCoordinator
                .getPartition(exportPartitionKey);
        if (globalPartition.isEmpty()) {
            throw new RuntimeException("Wait for global partition to be created.");
        }
        final GlobalState globalState = (GlobalState) globalPartition.get();
        final ExportLoadStatus exportLoadStatus = ExportLoadStatus.fromMap(globalState.getProgressState().get());
        exportLoadStatus.setTotalParitionsComplete(true);
        exportLoadStatus.setLastUpdateTimestamp(Instant.now().toEpochMilli());
        globalState.setProgressState(exportLoadStatus.toMap());
        enhancedSourceCoordinator.saveProgressStateForPartition(globalState, null);
    }

    private void closeExportPartitionWithError(final ExportPartition exportPartition) {
        LOG.error("The export from DocumentDB failed, it will be retried");
        exportJobFailureCounter.increment();
        final ExportProgressState exportProgressState = exportPartition.getProgressState().get();
        exportProgressState.setStatus(FAILED_STATUS);
        enhancedSourceCoordinator.closePartition(exportPartition, DEFAULT_CLOSE_DURATION, DEFAULT_MAX_CLOSE_COUNT);
    }

}
