/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.LoadStatus;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;


public class DataFileScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileScheduler.class);

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    /**
     * Maximum concurrent data loader per node
     */
    private static final int MAX_JOB_COUNT = 3;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 15_000;

    static final String EXPORT_S3_OBJECTS_PROCESSED_COUNT = "exportS3ObjectsProcessed";
    static final String ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE = "activeExportS3ObjectConsumers";


    private final EnhancedSourceCoordinator coordinator;

    private final ExecutorService executor;

    private final DataFileLoaderFactory loaderFactory;

    private final PluginMetrics pluginMetrics;


    private final Counter exportFileSuccessCounter;
    private final AtomicLong activeExportS3ObjectConsumersGauge;


    public DataFileScheduler(EnhancedSourceCoordinator coordinator, DataFileLoaderFactory loaderFactory, PluginMetrics pluginMetrics) {
        this.coordinator = coordinator;
        this.pluginMetrics = pluginMetrics;
        this.loaderFactory = loaderFactory;


        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);

        this.exportFileSuccessCounter = pluginMetrics.counter(EXPORT_S3_OBJECTS_PROCESSED_COUNT);
        this.activeExportS3ObjectConsumersGauge = pluginMetrics.gauge(ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE, new AtomicLong());
    }

    private void processDataFilePartition(DataFilePartition dataFilePartition) {
        String exportArn = dataFilePartition.getExportArn();
        String tableArn = getTableArn(exportArn);

        TableInfo tableInfo = getTableInfo(tableArn);

        Runnable loader = loaderFactory.createDataFileLoader(dataFilePartition, tableInfo);
        CompletableFuture runLoader = CompletableFuture.runAsync(loader, executor);
        runLoader.whenComplete(completeDataLoader(dataFilePartition));
        numOfWorkers.incrementAndGet();
    }

    @Override
    public void run() {
        LOG.debug("Start running Data File Scheduler");

        while (!Thread.currentThread().isInterrupted()) {
            if (numOfWorkers.get() < MAX_JOB_COUNT) {
                final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE);

                if (sourcePartition.isPresent()) {
                    activeExportS3ObjectConsumersGauge.incrementAndGet();
                    DataFilePartition dataFilePartition = (DataFilePartition) sourcePartition.get();
                    processDataFilePartition(dataFilePartition);
                    activeExportS3ObjectConsumersGauge.decrementAndGet();
                }
            }
            try {
                Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
            } catch (final InterruptedException e) {
                LOG.info("InterruptedException occurred");
                break;
            }

        }
        LOG.warn("Data file scheduler is interrupted, Stop all data file loaders...");
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

    private String getTableArn(String exportArn) {
        // e.g. given export arn:arn:aws:dynamodb:us-west-2:123456789012:table/Thread/export/01693291918297-bfeccbea
        // returns: arn:aws:dynamodb:us-west-2:123456789012:table/Thread
        return exportArn.substring(0, exportArn.lastIndexOf("/export/"));
    }

    private String getStreamArn(String exportArn) {
        String tableArn = getTableArn(exportArn);
        TableInfo tableInfo = getTableInfo(tableArn);

        if (tableInfo.getMetadata().isStreamRequired()) {
            return tableInfo.getMetadata().getStreamArn();
        }
        return null;
    }


    private BiConsumer completeDataLoader(DataFilePartition dataFilePartition) {
        return (v, ex) -> {
            numOfWorkers.decrementAndGet();
            if (ex == null) {
                exportFileSuccessCounter.increment();
                // Update global state
                updateState(dataFilePartition.getExportArn(), dataFilePartition.getProgressState().get().getLoaded());
                // After global state is updated, mask the partition as completed.
                coordinator.completePartition(dataFilePartition);

            } else {
                // The data loader must have already done one last checkpointing.
                LOG.debug("Data Loader completed with exception");
                LOG.error("{}", ex);
                // Release the ownership
                coordinator.giveUpPartition(dataFilePartition);
            }

        };
    }

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
            LOG.debug("Current status: total {} loaded {}", loadStatus.getTotalFiles(), loadStatus.getLoadedFiles());

            loadStatus.setLoadedFiles(loadStatus.getLoadedFiles() + 1);
            loadStatus.setLoadedRecords(loadStatus.getLoadedRecords() + loaded);
            globalState.setProgressState(loadStatus.toMap());

            try {
                coordinator.saveProgressStateForPartition(globalState);
                // if all load are completed.
                if (streamArn != null && loadStatus.getLoadedFiles() == loadStatus.getTotalFiles()) {
                    LOG.debug("All Exports are done, streaming can continue...");
                    coordinator.createPartition(new GlobalState(streamArn, Optional.empty()));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of dated, will retry..");
            }

        }


    }

}
