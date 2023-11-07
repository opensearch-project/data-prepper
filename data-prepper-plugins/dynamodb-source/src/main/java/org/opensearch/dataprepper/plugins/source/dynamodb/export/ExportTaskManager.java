/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeExportRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeExportResponse;
import software.amazon.awssdk.services.dynamodb.model.ExportFormat;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeRequest;
import software.amazon.awssdk.services.dynamodb.model.ExportTableToPointInTimeResponse;
import software.amazon.awssdk.services.dynamodb.model.S3SseAlgorithm;

import java.time.Instant;

public class ExportTaskManager {

    private static final Logger LOG = LoggerFactory.getLogger(ExportTaskManager.class);

    private static final ExportFormat DEFAULT_EXPORT_FORMAT = ExportFormat.ION;

    private final DynamoDbClient dynamoDBClient;


    public ExportTaskManager(DynamoDbClient dynamoDBClient) {
        this.dynamoDBClient = dynamoDBClient;
    }

    public String submitExportJob(String tableArn, String bucket, String prefix, String kmsKeyId, Instant exportTime) {
        S3SseAlgorithm algorithm = kmsKeyId == null || kmsKeyId.isEmpty() ? S3SseAlgorithm.AES256 : S3SseAlgorithm.KMS;
        // No needs to use a client token here.
        ExportTableToPointInTimeRequest req = ExportTableToPointInTimeRequest.builder()
                .tableArn(tableArn)
                .s3Bucket(bucket)
                .s3Prefix(prefix)
                .s3SseAlgorithm(algorithm)
                .s3SseKmsKeyId(kmsKeyId)
                .exportFormat(DEFAULT_EXPORT_FORMAT)
                .exportTime(exportTime)
                .build();


        try {
            ExportTableToPointInTimeResponse response = dynamoDBClient.exportTableToPointInTime(req);

            String exportArn = response.exportDescription().exportArn();
            String status = response.exportDescription().exportStatusAsString();
            LOG.debug("Export Job submitted with ARN {} and status {}", exportArn, status);
            return exportArn;
        } catch (SdkException e) {
            LOG.error("Failed to submit an export job with error " + e.getMessage());
            return null;
        }

    }

    public String getExportManifest(String exportArn) {
        DescribeExportRequest request = DescribeExportRequest.builder().exportArn(exportArn).build();

        String manifestKey = null;
        try {
            DescribeExportResponse resp = dynamoDBClient.describeExport(request);
            manifestKey = resp.exportDescription().exportManifest();

        } catch (SdkException e) {
            LOG.error("Unable to get manifest file for export " + exportArn);
        }
        return manifestKey;
    }

    public String checkExportStatus(String exportArn) {
        DescribeExportRequest request = DescribeExportRequest.builder().exportArn(exportArn).build();

        // Not need to catch exception here.
        DescribeExportResponse resp = dynamoDBClient.describeExport(request);
        String status = resp.exportDescription().exportStatusAsString();

        if (resp.exportDescription().failureCode() != null) {
            LOG.error("Export failed with error: " + resp.exportDescription().failureMessage());
        }

        return status;
    }


}
