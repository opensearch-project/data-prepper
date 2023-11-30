/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.DynamoDBSourceConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.LoadStatus;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;


public class DataFileScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileScheduler.class);

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    /**
     * Maximum concurrent data loader per node
     */
    private static final int MAX_JOB_COUNT = 1;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2_000;

    static final String EXPORT_S3_OBJECTS_PROCESSED_COUNT = "exportS3ObjectsProcessed";
    static final String ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE = "activeExportS3ObjectConsumers";


    private final EnhancedSourceCoordinator coordinator;

    private final ExecutorService executor;

    private final DataFileLoaderFactory loaderFactory;

    private final PluginMetrics pluginMetrics;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final DynamoDBSourceConfig dynamoDBSourceConfig;


    private final Counter exportFileSuccessCounter;
    private final AtomicInteger activeExportS3ObjectConsumersGauge;


    public DataFileScheduler(final EnhancedSourceCoordinator coordinator,
                             final DataFileLoaderFactory loaderFactory,
                             final PluginMetrics pluginMetrics,
                             final AcknowledgementSetManager acknowledgementSetManager,
                             final DynamoDBSourceConfig dynamoDBSourceConfig) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;
        this.loaderFactory = loaderFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.dynamoDBSourceConfig = dynamoDBSourceConfig;

        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);

        this.exportFileSuccessCounter = pluginMetrics.counter(EXPORT_S3_OBJECTS_PROCESSED_COUNT);
        this.activeExportS3ObjectConsumersGauge = pluginMetrics.gauge(ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE, numOfWorkers);
    }

    private void processDataFilePartition(DataFilePartition dataFilePartition) {
        String exportArn = dataFilePartition.getExportArn();
        String tableArn = TableUtil.getTableArnFromExportArn(exportArn);

        TableInfo tableInfo = getTableInfo(tableArn);

        final boolean acknowledgmentsEnabled = dynamoDBSourceConfig.isAcknowledgmentsEnabled();

        AcknowledgementSet acknowledgementSet = null;
        if (acknowledgmentsEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result) {
                    completeDataLoader(dataFilePartition).accept(null, null);
                    LOG.info("Received acknowledgment of completion from sink for data file {}", dataFilePartition.getKey());
                } else {
                    LOG.warn("Negative acknowledgment received for data file {}, retrying", dataFilePartition.getKey());
                    coordinator.giveUpPartition(dataFilePartition);
                }
            }, dynamoDBSourceConfig.getDataFileAcknowledgmentTimeout());
        }

        Runnable loader = loaderFactory.createDataFileLoader(dataFilePartition, tableInfo, acknowledgementSet, dynamoDBSourceConfig.getDataFileAcknowledgmentTimeout());
        CompletableFuture runLoader = CompletableFuture.runAsync(loader, executor);

        if (!acknowledgmentsEnabled) {
            runLoader.whenComplete(completeDataLoader(dataFilePartition));
        } else {
            runLoader.whenComplete((v, ex) -> {
                if (ex != null) {
                    coordinator.giveUpPartition(dataFilePartition);
                }
                numOfWorkers.decrementAndGet();
            });
        }
        numOfWorkers.incrementAndGet();
    }

    @Override
    public void run() {
        LOG.debug("Starting Data File Scheduler to process S3 data files for export");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (numOfWorkers.get() < MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE);

                    if (sourcePartition.isPresent()) {
                        DataFilePartition dataFilePartition = (DataFilePartition) sourcePartition.get();
                        processDataFilePartition(dataFilePartition);
                    }
                }
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The DataFileScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing an S3 data file, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The DataFileScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("Data file scheduler is interrupted, stopping all data file loaders...");
        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        executor.shutdown();
        DataFileLoader.stopAll();
    }

    private TableInfo getTableInfo(String tableArn) {
        GlobalState tableState = (GlobalState) coordinator.getPartition(tableArn).get();
        TableInfo tableInfo = new TableInfo(tableArn, TableMetadata.fromMap(tableState.getProgressState().get()));
        return tableInfo;
    }


    private String getStreamArn(String exportArn) {
        String tableArn = TableUtil.getTableArnFromExportArn(exportArn);
        TableInfo tableInfo = getTableInfo(tableArn);

        if (tableInfo.getMetadata().isStreamRequired()) {
            return tableInfo.getMetadata().getStreamArn();
        }
        return null;
    }


    private BiConsumer completeDataLoader(DataFilePartition dataFilePartition) {
        return (v, ex) -> {

            if (!dynamoDBSourceConfig.isAcknowledgmentsEnabled()) {
                numOfWorkers.decrementAndGet();
                if (numOfWorkers.get() == 0) {
                    activeExportS3ObjectConsumersGauge.decrementAndGet();
                }
            }
            if (ex == null) {
                exportFileSuccessCounter.increment();
                // Update global state
                updateState(dataFilePartition.getExportArn(), dataFilePartition.getProgressState().get().getLoaded());
                // After global state is updated, mask the partition as completed.
                coordinator.completePartition(dataFilePartition);

            } else {
                // The data loader must have already done one last checkpointing.
                LOG.error("Loading S3 data files completed with an exception: {}", ex);
                // Release the ownership
                coordinator.giveUpPartition(dataFilePartition);
            }

        };
    }

    /**
     * <p>There is a global state with sourcePartitionKey the export Arn,
     * to track the number of files are processed. </p>
     * <p>Each time, load of a data file is completed,
     * The state must be updated.</p>
     * Note that the state may be updated since multiple threads are updating the same state.
     * Retry is required.
     *
     * @param exportArn Export Arn.
     * @param loaded    Number records Loaded.
     */
    private void updateState(String exportArn, int loaded) {

        String streamArn = getStreamArn(exportArn);

        // Unlimited retries
        // The state be out of dated when updating.
        while (true) {
            Optional<EnhancedSourcePartition> globalPartition = coordinator.getPartition(exportArn);
            if (globalPartition.isEmpty()) {
                LOG.error("Failed to get load status for " + exportArn);
                return;
            }

            GlobalState globalState = (GlobalState) globalPartition.get();
            LoadStatus loadStatus = LoadStatus.fromMap(globalState.getProgressState().get());
            loadStatus.setLoadedFiles(loadStatus.getLoadedFiles() + 1);
            LOG.info("Current status: total {} loaded {}", loadStatus.getTotalFiles(), loadStatus.getLoadedFiles());

            loadStatus.setLoadedRecords(loadStatus.getLoadedRecords() + loaded);
            globalState.setProgressState(loadStatus.toMap());

            try {
                coordinator.saveProgressStateForPartition(globalState, null);
                // if all load are completed.
                if (streamArn != null && loadStatus.getLoadedFiles() == loadStatus.getTotalFiles()) {
                    LOG.info("All Exports are done, streaming can continue...");
                    coordinator.createPartition(new GlobalState(streamArn, Optional.empty()));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of date, will retry..");
            }
        }
    }

}
