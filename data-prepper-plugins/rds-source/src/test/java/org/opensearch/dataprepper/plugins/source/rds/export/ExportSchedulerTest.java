/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;


import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.ExportStatus;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotInfo;
import org.opensearch.dataprepper.plugins.source.rds.model.SnapshotStatus;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.DEFAULT_CLOSE_DURATION;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.DEFAULT_MAX_CLOSE_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.EXPORT_JOB_FAILURE_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.EXPORT_JOB_SUCCESS_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.EXPORT_S3_OBJECTS_TOTAL_COUNT;
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.PARQUET_SUFFIX;


@ExtendWith(MockitoExtension.class)
class ExportSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private SnapshotManager snapshotManager;

    @Mock
    private ExportTaskManager exportTaskManager;

    @Mock
    private S3Client s3Client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter exportJobSuccessCounter;

    @Mock
    private Counter exportJobFailureCounter;

    @Mock
    private Counter exportS3ObjectsTotalCounter;

    @Mock
    private ExportPartition exportPartition;

    @Mock(answer = Answers.RETURNS_DEFAULTS)
    private ExportProgressState exportProgressState;

    private ExportScheduler exportScheduler;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT)).thenReturn(exportJobSuccessCounter);
        when(pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT)).thenReturn(exportJobFailureCounter);
        when(pluginMetrics.counter(EXPORT_S3_OBJECTS_TOTAL_COUNT)).thenReturn(exportS3ObjectsTotalCounter);

        exportScheduler = createObjectUnderTest();
    }

    @Test
    void test_given_no_export_partition_then_not_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ExportPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(snapshotManager, exportTaskManager, s3Client, exportJobSuccessCounter,
                exportJobFailureCounter, exportS3ObjectsTotalCounter);
    }

    @Test
    void test_given_export_partition_and_export_task_id_then_complete_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.of(exportPartition));
        when(exportPartition.getPartitionKey()).thenReturn(UUID.randomUUID().toString());
        final String exportTaskId = UUID.randomUUID().toString();
        when(exportProgressState.getExportTaskId()).thenReturn(exportTaskId);
        when(exportPartition.getProgressState()).thenReturn(Optional.of(exportProgressState));
        when(exportTaskManager.checkExportStatus(exportTaskId)).thenReturn(ExportStatus.COMPLETE.name());

        // Mock list s3 objects response
        ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        String tableName = UUID.randomUUID().toString();
        // objectKey needs to have this structure: "{prefix}/{export task ID}/{database name}/{table name}/{numbered folder}/{file name}"
        S3Object s3Object = S3Object.builder()
                .key("prefix/" + exportTaskId + "/my_db/my_db." + tableName + "/1/file1" + PARQUET_SUFFIX)
                .build();
        when(listObjectsV2Response.contents()).thenReturn(List.of(s3Object));
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ExportPartition.PARTITION_TYPE));
        Thread.sleep(100);
        executorService.shutdownNow();

        verify(snapshotManager, never()).createSnapshot(any(String.class));
        verify(exportTaskManager, never()).startExportTask(
                any(String.class), any(String.class), any(String.class),
                any(String.class), any(String.class), any(List.class));
        verify(sourceCoordinator).createPartition(any(DataFilePartition.class));
        verify(sourceCoordinator).completePartition(exportPartition);
        verify(exportJobSuccessCounter).increment();
        verify(exportS3ObjectsTotalCounter).increment(1);
        verify(exportJobFailureCounter, never()).increment();
    }

    @Test
    void test_given_export_partition_without_export_task_id_then_start_and_complete_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.of(exportPartition));
        when(exportPartition.getPartitionKey()).thenReturn(UUID.randomUUID().toString());
        final String exportTaskId = UUID.randomUUID().toString();
        when(exportProgressState.getExportTaskId())
                .thenReturn(null)
                .thenReturn(exportTaskId);
        when(exportPartition.getProgressState()).thenReturn(Optional.of(exportProgressState));
        final String dbIdentifier = UUID.randomUUID().toString();
        when(exportPartition.getDbIdentifier()).thenReturn(dbIdentifier);

        // Mock snapshot response
        final String snapshotId = UUID.randomUUID().toString();
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotId;
        final Instant createTime = Instant.now();
        final SnapshotInfo snapshotInfoWhenCreate = new SnapshotInfo(
                snapshotId, snapshotArn, createTime, SnapshotStatus.CREATING.getStatusName());
        final SnapshotInfo snapshotInfoWhenComplete = new SnapshotInfo(
                snapshotId, snapshotArn, createTime, SnapshotStatus.AVAILABLE.getStatusName());
        when(snapshotManager.createSnapshot(dbIdentifier)).thenReturn(snapshotInfoWhenCreate);
        when(snapshotManager.checkSnapshotStatus(snapshotId)).thenReturn(snapshotInfoWhenComplete);

        // Mock export response
        when(exportProgressState.getIamRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getBucket()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getPrefix()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getKmsKeyId()).thenReturn(UUID.randomUUID().toString());
        when(exportTaskManager.startExportTask(any(String.class), any(String.class), any(String.class),
                any(String.class), any(String.class), any(List.class))).thenReturn(exportTaskId);
        when(exportTaskManager.checkExportStatus(exportTaskId)).thenReturn(ExportStatus.COMPLETE.name());

        // Mock list s3 objects response
        ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        String tableName = UUID.randomUUID().toString();
        // objectKey needs to have this structure: "{prefix}/{export task ID}/{database name}/{table name}/{numbered folder}/{file name}"
        S3Object s3Object = S3Object.builder()
                .key("prefix/" + exportTaskId + "/my_db/my_db." + tableName + "/1/file1" + PARQUET_SUFFIX)
                .build();
        when(listObjectsV2Response.contents()).thenReturn(List.of(s3Object));
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        // Act
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ExportPartition.PARTITION_TYPE));
        Thread.sleep(200);
        executorService.shutdownNow();

        // Assert
        verify(snapshotManager).createSnapshot(dbIdentifier);
        verify(exportTaskManager).startExportTask(
                any(String.class), any(String.class), any(String.class),
                any(String.class), any(String.class), any(List.class));
        verify(sourceCoordinator).createPartition(any(DataFilePartition.class));
        verify(sourceCoordinator).completePartition(exportPartition);
        verify(exportJobSuccessCounter).increment();
        verify(exportS3ObjectsTotalCounter).increment(1);
        verify(exportJobFailureCounter, never()).increment();
    }

    @Test
    void test_given_export_partition_and_null_export_task_id_then_close_partition_with_error() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.of(exportPartition));
        when(exportPartition.getPartitionKey()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getExportTaskId()).thenReturn(null);
        when(exportPartition.getProgressState()).thenReturn(Optional.of(exportProgressState));
        final String dbIdentifier = UUID.randomUUID().toString();
        when(exportPartition.getDbIdentifier()).thenReturn(dbIdentifier);

        // Mock snapshot response
        final String snapshotId = UUID.randomUUID().toString();
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotId;
        final Instant createTime = Instant.now();
        final SnapshotInfo snapshotInfoWhenCreate = new SnapshotInfo(
                snapshotId, snapshotArn, createTime, SnapshotStatus.CREATING.getStatusName());
        final SnapshotInfo snapshotInfoWhenComplete = new SnapshotInfo(
                snapshotId, snapshotArn, createTime, SnapshotStatus.AVAILABLE.getStatusName());
        when(snapshotManager.createSnapshot(dbIdentifier)).thenReturn(snapshotInfoWhenCreate);
        when(snapshotManager.checkSnapshotStatus(snapshotId)).thenReturn(snapshotInfoWhenComplete);

        // Mock export response
        when(exportProgressState.getIamRoleArn()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getBucket()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getPrefix()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getKmsKeyId()).thenReturn(UUID.randomUUID().toString());
        when(exportTaskManager.startExportTask(any(String.class), any(String.class), any(String.class),
                any(String.class), any(String.class), any(List.class))).thenReturn(null);

        // Act
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ExportPartition.PARTITION_TYPE));
        Thread.sleep(200);
        executorService.shutdownNow();

        // Assert
        verify(snapshotManager).createSnapshot(dbIdentifier);
        verify(exportTaskManager).startExportTask(
                any(String.class), any(String.class), any(String.class),
                any(String.class), any(String.class), any(List.class));
        verify(sourceCoordinator).closePartition(exportPartition, DEFAULT_CLOSE_DURATION, DEFAULT_MAX_CLOSE_COUNT);
        verify(sourceCoordinator, never()).createPartition(any(DataFilePartition.class));
        verify(sourceCoordinator, never()).completePartition(exportPartition);

        verify(exportJobFailureCounter).increment();
        verify(exportJobSuccessCounter, never()).increment();
        verify(exportS3ObjectsTotalCounter, never()).increment(1);
    }

    @Test
    void test_shutDown() {
        lenient().when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        exportScheduler.shutdown();
        verifyNoMoreInteractions(sourceCoordinator, snapshotManager, exportTaskManager, s3Client,
                exportJobSuccessCounter, exportJobFailureCounter, exportS3ObjectsTotalCounter);
        executorService.shutdownNow();
    }

    private ExportScheduler createObjectUnderTest() {
        return new ExportScheduler(sourceCoordinator, snapshotManager, exportTaskManager, s3Client, pluginMetrics);
    }
}