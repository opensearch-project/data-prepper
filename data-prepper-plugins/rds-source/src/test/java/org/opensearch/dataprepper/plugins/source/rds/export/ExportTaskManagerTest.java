/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.Stream;

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

    @ParameterizedTest
    @MethodSource("provideStartExportTaskTestParameters")
    void test_start_export_task(List<String> exportOnly) {
        final String snapshotArn = "arn:aws:rds:us-east-1:123456789012:snapshot:" + UUID.randomUUID();
        final String iamRoleArn = "arn:aws:iam:us-east-1:123456789012:role:" +  UUID.randomUUID();
        final String bucket = UUID.randomUUID().toString();
        final String prefix = UUID.randomUUID().toString();
        final String kmsKey = UUID.randomUUID().toString();

        exportTaskManager.startExportTask(snapshotArn, iamRoleArn, bucket, prefix, kmsKey, exportOnly);

        final ArgumentCaptor<StartExportTaskRequest> exportTaskRequestArgumentCaptor =
                ArgumentCaptor.forClass(StartExportTaskRequest.class);

        verify(rdsClient).startExportTask(exportTaskRequestArgumentCaptor.capture());

        final StartExportTaskRequest actualRequest = exportTaskRequestArgumentCaptor.getValue();
        assertThat(actualRequest.sourceArn(), equalTo(snapshotArn));
        assertThat(actualRequest.iamRoleArn(), equalTo(iamRoleArn));
        assertThat(actualRequest.s3BucketName(), equalTo(bucket));
        assertThat(actualRequest.s3Prefix(), equalTo(prefix));
        assertThat(actualRequest.kmsKeyId(), equalTo(kmsKey));
        assertThat(actualRequest.exportOnly(), equalTo(exportOnly));
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
        return new ExportTaskManager(rdsClient);
    }
}