/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.ExportRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.model.LoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.dataprepper.plugins.source.rds.RdsService.DATA_LOADER_MAX_JOB_COUNT;

public class DataFileScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DataFileScheduler.class);

    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2_000;

    private static final Duration DEFAULT_UPDATE_LOAD_STATUS_TIMEOUT = Duration.ofMinutes(30);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String EXPORT_S3_OBJECTS_PROCESSED_COUNT = "exportS3ObjectsProcessed";
    static final String ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE = "activeExportS3ObjectConsumers";


    private final EnhancedSourceCoordinator sourceCoordinator;
    private final ExecutorService executor;
    private final RdsSourceConfig sourceConfig;
    private final S3ObjectReader objectReader;
    private final InputCodec codec;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final ExportRecordConverter recordConverter;
    private final PluginMetrics pluginMetrics;

    private final Counter exportFileSuccessCounter;
    private final AtomicInteger activeExportS3ObjectConsumersGauge;

    private volatile boolean shutdownRequested = false;

    public DataFileScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                             final RdsSourceConfig sourceConfig,
                             final S3Client s3Client,
                             final EventFactory eventFactory,
                             final Buffer<Record<Event>> buffer,
                             final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        codec = new ParquetInputCodec(eventFactory);
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        objectReader = new S3ObjectReader(s3Client);
        recordConverter = new ExportRecordConverter();
        executor = Executors.newFixedThreadPool(DATA_LOADER_MAX_JOB_COUNT);
        this.pluginMetrics = pluginMetrics;

        this.exportFileSuccessCounter = pluginMetrics.counter(EXPORT_S3_OBJECTS_PROCESSED_COUNT);
        this.activeExportS3ObjectConsumersGauge = pluginMetrics.gauge(
                ACTIVE_EXPORT_S3_OBJECT_CONSUMERS_GAUGE, numOfWorkers, AtomicInteger::get);
    }

    @Override
    public void run() {
        LOG.debug("Starting Data File Scheduler to process S3 data files for export");

        while (!shutdownRequested && !Thread.currentThread().isInterrupted()) {
            try {
                if (numOfWorkers.get() < DATA_LOADER_MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(DataFilePartition.PARTITION_TYPE);

                    if (sourcePartition.isPresent()) {
                        LOG.debug("Acquired data file partition");
                        DataFilePartition dataFilePartition = (DataFilePartition) sourcePartition.get();
                        LOG.debug("Start processing data file partition");
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

        executor.shutdown();
    }

    public void shutdown() {
        shutdownRequested = true;
    }

    private void processDataFilePartition(DataFilePartition dataFilePartition) {
        Runnable loader = DataFileLoader.create(
                dataFilePartition, codec, bufferAccumulator, objectReader, recordConverter, pluginMetrics);
        CompletableFuture runLoader = CompletableFuture.runAsync(loader, executor);

        runLoader.whenComplete((v, ex) -> {
            if (ex == null) {
                exportFileSuccessCounter.increment();
                // Update global state so we know if all s3 files have been loaded
                updateLoadStatus(dataFilePartition.getExportTaskId(), DEFAULT_UPDATE_LOAD_STATUS_TIMEOUT);
                sourceCoordinator.completePartition(dataFilePartition);
            } else {
                LOG.error("There was an exception while processing an S3 data file", ex);
                sourceCoordinator.giveUpPartition(dataFilePartition);
            }
            numOfWorkers.decrementAndGet();
        });
        numOfWorkers.incrementAndGet();
    }

    private void updateLoadStatus(String exportTaskId, Duration timeout) {

        Instant endTime = Instant.now().plus(timeout);
        // Keep retrying in case update fails due to conflicts until timed out
        while (Instant.now().isBefore(endTime)) {
            Optional<EnhancedSourcePartition> globalStatePartition = sourceCoordinator.getPartition(exportTaskId);
            if (globalStatePartition.isEmpty()) {
                LOG.error("Failed to get data file load status for {}", exportTaskId);
                return;
            }

            GlobalState globalState = (GlobalState) globalStatePartition.get();
            LoadStatus loadStatus = LoadStatus.fromMap(globalState.getProgressState().get());
            loadStatus.setLoadedFiles(loadStatus.getLoadedFiles() + 1);
            LOG.info("Current data file load status: total {} loaded {}", loadStatus.getTotalFiles(), loadStatus.getLoadedFiles());

            globalState.setProgressState(loadStatus.toMap());

            try {
                sourceCoordinator.saveProgressStateForPartition(globalState, null);
                if (sourceConfig.isStreamEnabled() && loadStatus.getLoadedFiles() == loadStatus.getTotalFiles()) {
                    LOG.info("All exports are done, streaming can continue...");
                    sourceCoordinator.createPartition(new GlobalState("stream-for-" + sourceConfig.getDbIdentifier(), null));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of date, will retry..");
            }
        }
    }
}
