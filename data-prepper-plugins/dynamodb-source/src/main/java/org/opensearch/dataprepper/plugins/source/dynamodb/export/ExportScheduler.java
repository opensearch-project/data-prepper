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
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.ExportSummary;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.LoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

/**
 * A scheduler to manage all the export related work in one place
 */
public class ExportScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ExportScheduler.class);

    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;

    private static final Duration DEFAULT_CLOSE_DURATION = Duration.ofMinutes(10);

    private static final int DEFAULT_MAX_CLOSE_COUNT = 36;

    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 5 * 60_000;

    private static final int DEFAULT_CHECK_STATUS_INTERVAL_MILLS = 30 * 1000;


    private static final String COMPLETED_STATUS = "Completed";
    private static final String FAILED_STATUS = "Failed";

    static final String EXPORT_JOB_SUCCESS_COUNT = "exportJobSuccess";
    static final String EXPORT_JOB_FAILURE_COUNT = "exportJobFailure";
    static final String EXPORT_S3_OBJECTS_TOTAL_COUNT = "exportS3ObjectsTotal";
    static final String EXPORT_RECORDS_TOTAL_COUNT = "exportRecordsTotal";

    private final PluginMetrics pluginMetrics;

    private final EnhancedSourceCoordinator enhancedSourceCoordinator;

    private final DynamoDbClient dynamoDBClient;

    private final ExecutorService executor;

    private final ManifestFileReader manifestFileReader;

    private final ExportTaskManager exportTaskManager;

    private final Counter exportJobSuccessCounter;
    private final Counter exportJobFailureCounter;

    private final Counter exportS3ObjectsTotalCounter;
    private final Counter exportRecordsTotalCounter;

    public ExportScheduler(EnhancedSourceCoordinator enhancedSourceCoordinator, DynamoDbClient dynamoDBClient, ManifestFileReader manifestFileReader, PluginMetrics pluginMetrics) {
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.dynamoDBClient = dynamoDBClient;
        this.pluginMetrics = pluginMetrics;
        this.exportTaskManager = new ExportTaskManager(dynamoDBClient);

        this.manifestFileReader = manifestFileReader;
        executor = Executors.newCachedThreadPool();

        exportJobSuccessCounter = pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT);
        exportJobFailureCounter = pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT);
        exportS3ObjectsTotalCounter = pluginMetrics.counter(EXPORT_S3_OBJECTS_TOTAL_COUNT);
        exportRecordsTotalCounter = pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT);


    }

    @Override
    public void run() {
        LOG.debug("Start running Export Scheduler");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Does not have limit on max leases
                // As most of the time it's just to wait
                final Optional<EnhancedSourcePartition> sourcePartition = enhancedSourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE);

                if (sourcePartition.isPresent()) {

                    ExportPartition exportPartition = (ExportPartition) sourcePartition.get();
                    LOG.debug("Acquired an export partition: " + exportPartition.getPartitionKey());

                    String exportArn = getOrCreateExportArn(exportPartition);

                    if (exportArn == null) {
                        closeExportPartitionWithError(exportPartition);
                    } else {
                        CompletableFuture<String> checkStatus = CompletableFuture.supplyAsync(() -> checkExportStatus(exportPartition), executor);
                        checkStatus.whenComplete(completeExport(exportPartition));
                    }

                }
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception during export from DynamoDB to S3, backing off and retrying", e);
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The ExportScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("Export scheduler interrupted, looks like shutdown has triggered");
        executor.shutdownNow();

    }


    private BiConsumer<String, Throwable> completeExport(ExportPartition exportPartition) {
        return (status, ex) -> {
            if (ex != null) {
                LOG.warn("Check export status for {} failed with error {}", exportPartition.getPartitionKey(), ex.getMessage());
//                closeExportPartitionWithError(exportPartition);
                enhancedSourceCoordinator.giveUpPartition(exportPartition);
            } else {
                // check status
                LOG.debug("Check export status completed successfully");

                if (!"COMPLETED".equals(status)) {
                    closeExportPartitionWithError(exportPartition);
                    return;
                }
                LOG.debug("Start reading the manifest files");

                // Always has a state
                ExportProgressState state = exportPartition.getProgressState().get();
                String bucketName = state.getBucket();
                String exportArn = state.getExportArn();

                String manifestKey = exportTaskManager.getExportManifest(exportArn);
                LOG.debug("Export manifest summary file is " + manifestKey);

                // Extract the info in the manifest summary file
                // We may need to store the info
                ExportSummary summaryInfo = manifestFileReader.parseSummaryFile(bucketName, manifestKey);

                // Get the manifest data path
                // We don't really need to use the summary info to get the path
                Map<String, Integer> dataFileInfo = manifestFileReader.parseDataFile(summaryInfo.getS3Bucket(), summaryInfo.getManifestFilesS3Key());

                // Create a data file partition for each
                createDataFilePartitions(exportArn, bucketName, dataFileInfo);

                // Finally close the export partition
                completeExportPartition(exportPartition);

            }

        };
    }


    private void createDataFilePartitions(String exportArn, String bucketName, Map<String, Integer> dataFileInfo) {
        LOG.info("Total of {} data files generated for export {}", dataFileInfo.size(), exportArn);
        AtomicInteger totalRecords = new AtomicInteger();
        AtomicInteger totalFiles = new AtomicInteger();
        dataFileInfo.forEach((key, size) -> {
            DataFileProgressState progressState = new DataFileProgressState();
            progressState.setTotal(size);
            progressState.setLoaded(0);

            totalFiles.addAndGet(1);
            totalRecords.addAndGet(size);
            DataFilePartition partition = new DataFilePartition(exportArn, bucketName, key, Optional.of(progressState));
            enhancedSourceCoordinator.createPartition(partition);
        });

        exportS3ObjectsTotalCounter.increment(totalFiles.get());
        exportRecordsTotalCounter.increment(totalRecords.get());

        // Currently, we need to maintain a global state to track the overall progress.
        // So that we can easily tell if all the export files are loaded
        LoadStatus loadStatus = new LoadStatus(totalFiles.get(), 0, totalRecords.get(), 0);
        enhancedSourceCoordinator.createPartition(new GlobalState(exportArn, Optional.of(loadStatus.toMap())));
    }


    private void closeExportPartitionWithError(ExportPartition exportPartition) {
        LOG.error("The export from DynamoDb to S3 failed, it will be retried");
        exportJobFailureCounter.increment(1);
        ExportProgressState exportProgressState = exportPartition.getProgressState().get();
        // Clear current Arn, so that a new export can be submitted.
        exportProgressState.setExportArn(null);
        exportProgressState.setStatus(FAILED_STATUS);
        enhancedSourceCoordinator.closePartition(exportPartition, DEFAULT_CLOSE_DURATION, DEFAULT_MAX_CLOSE_COUNT);
    }

    private void completeExportPartition(ExportPartition exportPartition) {
        exportJobSuccessCounter.increment();
        ExportProgressState state = exportPartition.getProgressState().get();
        state.setStatus(COMPLETED_STATUS);
        enhancedSourceCoordinator.completePartition(exportPartition);
    }

    private String checkExportStatus(ExportPartition exportPartition) {
        long lastCheckpointTime = System.currentTimeMillis();
        String exportArn = exportPartition.getProgressState().get().getExportArn();

        LOG.debug("Start Checking the status of export " + exportArn);
        while (true) {
            if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                enhancedSourceCoordinator.saveProgressStateForPartition(exportPartition, null);
                lastCheckpointTime = System.currentTimeMillis();
            }

            String status = exportTaskManager.checkExportStatus(exportArn);
            if (!"IN_PROGRESS".equals(status)) {
                LOG.info("Export {} is completed with final status {}", exportArn, status);
                return status;
            }
            LOG.debug("Export {} is still running in progress, sleep and recheck later", exportArn);
            try {
                Thread.sleep(DEFAULT_CHECK_STATUS_INTERVAL_MILLS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private String getOrCreateExportArn(ExportPartition exportPartition) {
        // State cannot be empty
        ExportProgressState state = exportPartition.getProgressState().get();
        // Check the progress state
        if (state.getExportArn() != null) {
            LOG.info("Export Job has already submitted for table {} with export time {}", exportPartition.getTableArn(), exportPartition.getExportTime());
            // Export job already submitted
            return state.getExportArn();
        }

        LOG.info("Try to submit a new export job for table {} with export time {}", exportPartition.getTableArn(), exportPartition.getExportTime());
        // submit a new export request
        String exportArn = exportTaskManager.submitExportJob(exportPartition.getTableArn(), state.getBucket(), state.getPrefix(), state.getKmsKeyId(), exportPartition.getExportTime());

        // Update state with export Arn in the coordination table.
        // So that it won't be submitted again after a restart.
        if (exportArn != null) {
            LOG.info("Export arn is " + exportArn);
            state.setExportArn(exportArn);
            enhancedSourceCoordinator.saveProgressStateForPartition(exportPartition, null);
        }
        return exportArn;
    }

}
