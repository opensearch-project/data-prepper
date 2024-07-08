/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksRequest;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksResponse;
import software.amazon.awssdk.services.rds.model.StartExportTaskRequest;
import software.amazon.awssdk.services.rds.model.StartExportTaskResponse;
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
import static org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler.PARQUET_SUFFIX;


@ExtendWith(MockitoExtension.class)
class ExportSchedulerTest {

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private S3Client s3Client;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExportPartition exportPartition;

    @Mock(answer = Answers.RETURNS_DEFAULTS)
    private ExportProgressState exportProgressState;

    private ExportScheduler exportScheduler;

    @BeforeEach
    void setUp() {
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

        verifyNoInteractions(rdsClient);
    }

    @Test
    void test_given_export_partition_and_task_id_then_complete_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.of(exportPartition));
        when(exportPartition.getPartitionKey()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getExportTaskId()).thenReturn(UUID.randomUUID().toString());
        when(exportPartition.getProgressState()).thenReturn(Optional.of(exportProgressState));

        DescribeExportTasksResponse describeExportTasksResponse = mock(DescribeExportTasksResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(describeExportTasksResponse.exportTasks().get(0).status()).thenReturn("COMPLETE");
        when(rdsClient.describeExportTasks(any(DescribeExportTasksRequest.class))).thenReturn(describeExportTasksResponse);

        // Mock list s3 objects response
        ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        String exportTaskId = UUID.randomUUID().toString();
        String tableName = UUID.randomUUID().toString();
        // objectKey needs to have this structure: "{prefix}/{export task ID}/{database name}/{table name}/..."
        S3Object s3Object = S3Object.builder()
                .key("prefix/" + exportTaskId + "/my_db/" + tableName + PARQUET_SUFFIX)
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

        verify(sourceCoordinator).createPartition(any(DataFilePartition.class));
        verify(sourceCoordinator).completePartition(exportPartition);
        verify(rdsClient, never()).startExportTask(any(StartExportTaskRequest.class));
        verify(rdsClient, never()).createDBSnapshot(any(CreateDbSnapshotRequest.class));
    }


    @Test
    void test_given_export_partition_without_task_id_then_start_and_complete_export() throws InterruptedException {
        when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.of(exportPartition));
        when(exportPartition.getPartitionKey()).thenReturn(UUID.randomUUID().toString());
        when(exportProgressState.getExportTaskId()).thenReturn(null).thenReturn(UUID.randomUUID().toString());
        when(exportPartition.getProgressState()).thenReturn(Optional.of(exportProgressState));
        final String dbIdentifier = UUID.randomUUID().toString();
        when(exportPartition.getDbIdentifier()).thenReturn(dbIdentifier);

        // Mock snapshot response
        CreateDbSnapshotResponse createDbSnapshotResponse = mock(CreateDbSnapshotResponse.class);
        DBSnapshot dbSnapshot = mock(DBSnapshot.class);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:snapshot-0b5ae174";
        when(dbSnapshot.dbSnapshotArn()).thenReturn(snapshotArn);
        when(dbSnapshot.status()).thenReturn("creating").thenReturn("available");
        when(dbSnapshot.snapshotCreateTime()).thenReturn(Instant.now());
        when(createDbSnapshotResponse.dbSnapshot()).thenReturn(dbSnapshot);
        when(rdsClient.createDBSnapshot(any(CreateDbSnapshotRequest.class))).thenReturn(createDbSnapshotResponse);

        DescribeDbSnapshotsResponse describeDbSnapshotsResponse = DescribeDbSnapshotsResponse.builder()
                .dbSnapshots(dbSnapshot)
                .build();
        when(rdsClient.describeDBSnapshots(any(DescribeDbSnapshotsRequest.class))).thenReturn(describeDbSnapshotsResponse);

        // Mock export response
        StartExportTaskResponse startExportTaskResponse = mock(StartExportTaskResponse.class);
        when(startExportTaskResponse.status()).thenReturn("STARTING");
        when(rdsClient.startExportTask(any(StartExportTaskRequest.class))).thenReturn(startExportTaskResponse);

        DescribeExportTasksResponse describeExportTasksResponse = mock(DescribeExportTasksResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(describeExportTasksResponse.exportTasks().get(0).status()).thenReturn("COMPLETE");
        when(rdsClient.describeExportTasks(any(DescribeExportTasksRequest.class))).thenReturn(describeExportTasksResponse);

        // Mock list s3 objects response
        ListObjectsV2Response listObjectsV2Response = mock(ListObjectsV2Response.class);
        String exportTaskId = UUID.randomUUID().toString();
        String tableName = UUID.randomUUID().toString();
        // objectKey needs to have this structure: "{prefix}/{export task ID}/{database name}/{table name}/..."
        S3Object s3Object = S3Object.builder()
                .key("prefix/" + exportTaskId + "/my_db/" + tableName + PARQUET_SUFFIX)
                .build();
        when(listObjectsV2Response.contents()).thenReturn(List.of(s3Object));
        when(listObjectsV2Response.isTruncated()).thenReturn(false);
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> verify(sourceCoordinator).acquireAvailablePartition(ExportPartition.PARTITION_TYPE));
        Thread.sleep(200);
        executorService.shutdownNow();

        verify(rdsClient).createDBSnapshot(any(CreateDbSnapshotRequest.class));
        verify(rdsClient).startExportTask(any(StartExportTaskRequest.class));
        verify(sourceCoordinator).createPartition(any(DataFilePartition.class));
        verify(sourceCoordinator).completePartition(exportPartition);
    }

    @Test
    void test_shutDown() {
        lenient().when(sourceCoordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(exportScheduler);
        exportScheduler.shutdown();
        verifyNoMoreInteractions(sourceCoordinator, rdsClient);
        executorService.shutdownNow();
    }

    private ExportScheduler createObjectUnderTest() {
        return new ExportScheduler(sourceCoordinator, rdsClient, s3Client, pluginMetrics);
    }
}