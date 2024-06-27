/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksRequest;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksResponse;
import software.amazon.awssdk.services.rds.model.ExportTask;
import software.amazon.awssdk.services.rds.model.StartExportTaskRequest;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ExportTaskManagerTest {

    @Mock
    private RdsClient rdsClient;

    private ExportTaskManager exportTaskManager;

    @BeforeEach
    void setUp() {
        exportTaskManager = createObjectUnderTest();
    }

    @Test
    void test_start_export_task_without_specifying_tables() {
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:snapshot-0b5ae174";
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:my-role";
        final String bucket = "bucket";
        final String prefix = "prefix";
        final String kmsKey = UUID.randomUUID().toString();
        final List<String> exportOnly = List.of();

        exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);


        final ArgumentCaptor<StartExportTaskRequest> exportTaskRequestArgumentCaptor =
                ArgumentCaptor.forClass(StartExportTaskRequest.class);

        verify(rdsClient).startExportTask(exportTaskRequestArgumentCaptor.capture());

        final StartExportTaskRequest actualRequest = exportTaskRequestArgumentCaptor.getValue();
        assertRequestParameters(actualRequest, snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);
    }

    @Test
    void test_start_export_task_without_specifying_tables1() {
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:snapshot-0b5ae174";
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:my-role";
        final String bucket = "bucket";
        final String prefix = "prefix";
        final String kmsKey = UUID.randomUUID().toString();
        final List<String> exportOnly = List.of("my_db.cars", "my_db.houses");

        exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);

        final ArgumentCaptor<StartExportTaskRequest> exportTaskRequestArgumentCaptor =
                ArgumentCaptor.forClass(StartExportTaskRequest.class);

        verify(rdsClient).startExportTask(exportTaskRequestArgumentCaptor.capture());

        final StartExportTaskRequest actualRequest = exportTaskRequestArgumentCaptor.getValue();
        assertRequestParameters(actualRequest, snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);
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

    private ExportTaskManager createObjectUnderTest() {
        return new ExportTaskManager(rdsClient);
    }

    private void assertRequestParameters(final StartExportTaskRequest request,
                                         final String sourceArn,
                                         final String iamRoleArn,
                                         final String bucket,
                                         final String prefix,
                                         final String kmsKey,
                                         final List<String> exportOnly) {
        assertThat(request.sourceArn(), equalTo(sourceArn));
        assertThat(request.iamRoleArn(), equalTo(iamRoleArn));
        assertThat(request.s3BucketName(), equalTo(bucket));
        assertThat(request.s3Prefix(), equalTo(prefix));
        assertThat(request.kmsKeyId(), equalTo(kmsKey));
        assertThat(request.exportOnly(), equalTo(exportOnly));
    }
}