/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Model represents the summary manifest information of the export.
 * <p>
 * Check more details from
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html#S3DataExport.Output_Manifest
 * </p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExportSummary {
    @JsonProperty("version")
    private String version;

    @JsonProperty("exportArn")
    private String exportArn;

    @JsonProperty("startTime")
    private String startTime;

    @JsonProperty("endTime")
    private String endTime;

    @JsonProperty("tableArn")
    private String tableArn;

    @JsonProperty("tableId")
    private String tableId;

    @JsonProperty("exportTime")
    private String exportTime;

    @JsonProperty("s3Bucket")
    private String s3Bucket;

    @JsonProperty("s3Prefix")
    private String s3Prefix;

    @JsonProperty("s3SseAlgorithm")
    private String s3SseAlgorithm;

    @JsonProperty("s3SseKmsKeyId")
    private String s3SseKmsKeyId;

    @JsonProperty("manifestFilesS3Key")
    private String manifestFilesS3Key;

    @JsonProperty("billedSizeBytes")
    private long billedSizeBytes;

    @JsonProperty("itemCount")
    private long itemCount;

    @JsonProperty("outputFormat")
    private String outputFormat;


    public String getVersion() {
        return version;
    }

    public String getExportArn() {
        return exportArn;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getTableArn() {
        return tableArn;
    }

    public String getTableId() {
        return tableId;
    }

    public String getExportTime() {
        return exportTime;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public String getS3SseAlgorithm() {
        return s3SseAlgorithm;
    }

    public String getS3SseKmsKeyId() {
        return s3SseKmsKeyId;
    }

    public String getManifestFilesS3Key() {
        return manifestFilesS3Key;
    }

    public long getBilledSizeBytes() {
        return billedSizeBytes;
    }

    public long getItemCount() {
        return itemCount;
    }

    public String getOutputFormat() {
        return outputFormat;
    }
}
