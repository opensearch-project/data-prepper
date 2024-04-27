/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksRequest;
import software.amazon.awssdk.services.rds.model.DescribeExportTasksResponse;
import software.amazon.awssdk.services.rds.model.StartExportTaskRequest;
import software.amazon.awssdk.services.rds.model.StartExportTaskResponse;

import java.util.Collection;
import java.util.UUID;

public class ExportTaskManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExportTaskManager.class);

    // Export identifier cannot be longer than 60 characters
    private static final int EXPORT_TASK_ID_MAX_LENGTH = 60;

    private final RdsClient rdsClient;

    public ExportTaskManager(final RdsClient rdsClient) {
        this.rdsClient = rdsClient;
    }

    public String startExportTask(String snapshotArn, String iamRoleArn, String bucket, String prefix, String kmsKeyId, Collection<String> includeTables) {
        final String exportTaskId = generateExportTaskId(snapshotArn);
        StartExportTaskRequest.Builder requestBuilder = StartExportTaskRequest.builder()
                .exportTaskIdentifier(exportTaskId)
                .sourceArn(snapshotArn)
                .iamRoleArn(iamRoleArn)
                .s3BucketName(bucket)
                .s3Prefix(prefix)
                .kmsKeyId(kmsKeyId);

        if (includeTables != null && !includeTables.isEmpty()) {
            requestBuilder.exportOnly(includeTables);
        }

        try {
            StartExportTaskResponse response = rdsClient.startExportTask(requestBuilder.build());
            LOG.info("Export task submitted with id {} and status {}", exportTaskId, response.status());
            return exportTaskId;

        } catch (Exception e) {
            LOG.error("Failed to start an export task", e);
            return null;
        }
    }

    public String checkExportStatus(String exportTaskId) {
        DescribeExportTasksRequest request = DescribeExportTasksRequest.builder()
                .exportTaskIdentifier(exportTaskId)
                .build();

        DescribeExportTasksResponse response = rdsClient.describeExportTasks(request);

        return response.exportTasks().get(0).status();
    }

    private String generateExportTaskId(String snapshotArn) {
        String snapshotId = Arn.fromString(snapshotArn).resource().resource();
        return truncateString(snapshotId, EXPORT_TASK_ID_MAX_LENGTH - 16) + "-export-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String truncateString(String originalString, int maxLength) {
        if (originalString.length() <= maxLength) {
            return originalString;
        }
        return originalString.substring(0, maxLength);
    }
}
