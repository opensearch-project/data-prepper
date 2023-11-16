/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.ExportSummary;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeExportRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeExportResponse;
import software.amazon.awssdk.services.dynamodb.model.ExportDescription;
import software.amazon.awssdk.services.dynamodb.model.ExportFormat;
import software.amazon.awssdk.services.dynamodb.model.ExportStatus;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeRequest;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeResponse;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler.EXPORT_JOB_FAILURE_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler.EXPORT_JOB_SUCCESS_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler.EXPORT_RECORDS_TOTAL_COUNT;
import static org.opensearch.dataprepper.plugins.source.dynamodb.export.ExportScheduler.EXPORT_S3_OBJECTS_TOTAL_COUNT;


@ExtendWith(MockitoExtension.class)
class ExportSchedulerTest {


    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private DynamoDbClient dynamoDBClient;

    @Mock
    private ManifestFileReader manifestFileReader;

    @Mock
    private PluginMetrics pluginMetrics;

    private ExportScheduler scheduler;

    @Mock
    private ExportPartition exportPartition;


    @Mock
    private Counter exportJobSuccess;

    @Mock
    private Counter exportJobErrors;

    @Mock
    private Counter exportFilesTotal;

    @Mock
    private Counter exportRecordsTotal;

    private final String tableName = UUID.randomUUID().toString();
    private final String tableArn = "arn:aws:dynamodb:us-west-2:123456789012:table/" + tableName;

    private final String manifestKey = UUID.randomUUID().toString();
    private final String bucketName = UUID.randomUUID().toString();
    private final String prefix = UUID.randomUUID().toString();

    private final String exportArn = tableArn + "/export/01693291918297-bfeccbea";

    private final long exportTimeMills = 1695021857760L;
    private final Instant exportTime = Instant.ofEpochMilli(exportTimeMills);


    @BeforeEach
    void setup() {

        ExportSummary summary = mock(ExportSummary.class);
        lenient().when(manifestFileReader.parseSummaryFile(anyString(), anyString())).thenReturn(summary);
        lenient().when(summary.getS3Bucket()).thenReturn(bucketName);
        lenient().when(summary.getManifestFilesS3Key()).thenReturn(manifestKey);
        lenient().when(summary.getExportTime()).thenReturn(exportTime.toString());
        lenient().when(manifestFileReader.parseDataFile(anyString(), anyString())).thenReturn(Map.of("Key1", 100, "Key2", 200));

        lenient().when(coordinator.createPartition(any(EnhancedSourcePartition.class))).thenReturn(true);
        lenient().doNothing().when(coordinator).completePartition(any(EnhancedSourcePartition.class));
        lenient().doNothing().when(coordinator).giveUpPartition(any(EnhancedSourcePartition.class));

    }


    @Test
    public void test_run_exportJob_correctly() throws InterruptedException {
        when(exportPartition.getTableArn()).thenReturn(tableArn);
        when(exportPartition.getExportTime()).thenReturn(exportTime);

        ExportProgressState state = new ExportProgressState();
        state.setBucket(bucketName);
        state.setPrefix(prefix);
        when(exportPartition.getProgressState()).thenReturn(Optional.of(state));

        given(pluginMetrics.counter(EXPORT_JOB_SUCCESS_COUNT)).willReturn(exportJobSuccess);
        given(pluginMetrics.counter(EXPORT_JOB_FAILURE_COUNT)).willReturn(exportJobErrors);
        given(pluginMetrics.counter(EXPORT_S3_OBJECTS_TOTAL_COUNT)).willReturn(exportFilesTotal);
        given(pluginMetrics.counter(EXPORT_RECORDS_TOTAL_COUNT)).willReturn(exportRecordsTotal);

        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willReturn(Optional.of(exportPartition)).willReturn(Optional.empty());

        // Set up mock behavior
        ExportDescription desc = ExportDescription.builder()
                .exportArn(exportArn)
                .exportStatus(ExportStatus.COMPLETED)
                .exportFormat(ExportFormat.ION)
                .exportManifest(manifestKey)
                .build();

        ExportTableToPointInTimeResponse exportResponse = ExportTableToPointInTimeResponse.builder()
                .exportDescription(desc)
                .build();

        final ArgumentCaptor<ExportTableToPointInTimeRequest> exportRequestArgumentCaptor = ArgumentCaptor.forClass(ExportTableToPointInTimeRequest.class);
        when(dynamoDBClient.exportTableToPointInTime(exportRequestArgumentCaptor.capture())).thenReturn(exportResponse);
        DescribeExportResponse describeExportResponse = DescribeExportResponse.builder().exportDescription(desc).build();
        when(dynamoDBClient.describeExport(any(DescribeExportRequest.class))).thenReturn(describeExportResponse);

        scheduler = new ExportScheduler(coordinator, dynamoDBClient, manifestFileReader, pluginMetrics);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(scheduler);

        Thread.sleep(500);

        verify(dynamoDBClient).exportTableToPointInTime(any(ExportTableToPointInTimeRequest.class));
        verify(dynamoDBClient, times(2)).describeExport(any(DescribeExportRequest.class));

        // Create 2 data file partitions + 1 global state
        verify(coordinator, times(3)).createPartition(any(EnhancedSourcePartition.class));
        // Complete the export partition
        verify(coordinator).completePartition(any(EnhancedSourcePartition.class));
        verify(exportJobSuccess).increment();
        verify(exportFilesTotal).increment(2);
        verify(exportRecordsTotal).increment(300);
        verifyNoInteractions(exportJobErrors);

        executor.shutdownNow();

    }

    @Test
    void run_catches_exception_and_retries_when_exception_is_thrown_during_processing() throws InterruptedException {
        given(coordinator.acquireAvailablePartition(ExportPartition.PARTITION_TYPE)).willThrow(RuntimeException.class);

        scheduler = new ExportScheduler(coordinator, dynamoDBClient, manifestFileReader, pluginMetrics);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> future = executorService.submit(() -> scheduler.run());
        Thread.sleep(100);
        assertThat(future.isDone(), equalTo(false));
        executorService.shutdown();
        future.cancel(true);
        assertThat(executorService.awaitTermination(1000, TimeUnit.MILLISECONDS), equalTo(true));
    }

}