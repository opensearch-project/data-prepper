/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.utils.RdsSourceAggregateMetrics;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksRequest;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksResponse;
import software.amazon.awssdk.services.rds.model.ExportTask;
import software.amazon.awssdk.services.rds.model.StartExportTaskRequest;
import software.amazon.awssdk.services.rds.model.StartExportTaskResponse;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ExportTaskManagerTest {

    @Mock
    private RdsClient rdsClient;

    @Mock
    private RdsSourceAggregateMetrics rdsSourceAggregateMetrics;

    @Mock
    private Counter exportApiInvocations;

    @Mock
    private Counter export4xxErrors;

    @Mock
    private Counter export5xxErrors;

    private ExportTaskManager exportTaskManager;

    @BeforeEach
    void setUp() {
        exportTaskManager = createObjectUnderTest();
        lenient().when(rdsSourceAggregateMetrics.getExportApiInvocations()).thenReturn(exportApiInvocations);
        lenient().when(rdsSourceAggregateMetrics.getExport4xxErrors()).thenReturn(export4xxErrors);
        lenient().when(rdsSourceAggregateMetrics.getExport5xxErrors()).thenReturn(export5xxErrors);
    }

    @ParameterizedTest
    @MethodSource("provideStartExportTaskTestParameters")
    void test_start_export_task(List<String> exportOnly) {
        final String snapshotId = UUID.randomUUID().toString().substring(0, 5);
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + snapshotId;
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:" +  UUID.randomUUID();
        final String bucket = UUID.randomUUID().toString();
        final String prefix = UUID.randomUUID().toString();
        final String kmsKey = UUID.randomUUID().toString();
        final StartExportTaskResponse response = mock(StartExportTaskResponse.class);
        when(rdsClient.startExportTask(any(StartExportTaskRequest.class))).thenReturn(response);
        when(response.status()).thenReturn(UUID.randomUUID().toString());

        final String exportTaskId = exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);

        final ArgumentCaptor<StartExportTaskRequest> exportTaskRequestArgumentCaptor =
                ArgumentCaptor.forClass(StartExportTaskRequest.class);

        assertThat(exportTaskId, startsWith(snapshotId));
        verify(rdsClient).startExportTask(exportTaskRequestArgumentCaptor.capture());
        verify(exportApiInvocations).increment();

        final StartExportTaskRequest actualRequest = exportTaskRequestArgumentCaptor.getValue();
        assertThat(actualRequest.sourceArn(), equalTo(snapshotArn));
        assertThat(actualRequest.iamRoleArn(), equalTo(iamRoleArn));
        assertThat(actualRequest.s3BucketName(), equalTo(bucket));
        assertThat(actualRequest.s3Prefix(), equalTo(prefix));
        assertThat(actualRequest.kmsKeyId(), equalTo(kmsKey));
        assertThat(actualRequest.exportOnly(), equalTo(exportOnly));
    }

    @Test
    void test_start_export_task_with_sdkexception_returns_null() {
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + UUID.randomUUID();
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:" +  UUID.randomUUID();
        final String bucket = UUID.randomUUID().toString();
        final String prefix = UUID.randomUUID().toString();
        final String kmsKey = UUID.randomUUID().toString();

        when(rdsClient.startExportTask(any(StartExportTaskRequest.class))).thenThrow(SdkException.class);

        final String exportTaskId = exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, List.of());

        assertThat(exportTaskId, equalTo(null));
        verify(exportApiInvocations).increment();
        verify(export4xxErrors).increment();
    }

    @Test
    void test_start_export_task_with_other_exception_returns_null() {
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + UUID.randomUUID();
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:" +  UUID.randomUUID();
        final String bucket = UUID.randomUUID().toString();
        final String prefix = UUID.randomUUID().toString();
        final String kmsKey = UUID.randomUUID().toString();

        when(rdsClient.startExportTask(any(StartExportTaskRequest.class))).thenThrow(RuntimeException.class);

        final String exportTaskId = exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, List.of());

        assertThat(exportTaskId, equalTo(null));
        verify(exportApiInvocations).increment();
        verify(export5xxErrors).increment();
    }

    @Test
    void test_check_export_status() {
        final String exportTaskId = UUID.randomUUID().toString();
        DescribeExportTasksResponse describeExportTasksResponse = mock(DescribeExportTasksResponse.class);
        when(describeExportTasksResponse.exportTasks()).thenReturn(List.of(ExportTask.builder().status("COMPLETE").build()));
        when(rdsClient.describeExportTasks(any(DescribeExportTasksRequest.class))).thenReturn(describeExportTasksResponse);

        exportTaskManager.checkExportStatus(exportTaskId);

        final ArgumentCaptor<DescribeExportTasksRequest> exportTaskRequestArgumentCaptor =
                ArgumentCaptor.forClass(DescribeExportTasksRequest.class);

        verify(rdsClient).describeExportTasks(exportTaskRequestArgumentCaptor.capture());

        final DescribeExportTasksRequest actualRequest = exportTaskRequestArgumentCaptor.getValue();
        assertThat(actualRequest.exportTaskIdentifier(), equalTo(exportTaskId));
    }

    private static Stream<Arguments> provideStartExportTaskTestParameters() {
        final String tableName1 = UUID.randomUUID().toString();
        final String tableName2 = UUID.randomUUID().toString();
        return Stream.of(
                Arguments.of(List.of()),
                Arguments.of(List.of(tableName1)),
                Arguments.of(List.of(tableName1, tableName2))
        );
    }

    private ExportTaskManager createObjectUnderTest() {
        return new ExportTaskManager(rdsClient, rdsSourceAggregateMetrics);
    }
}