/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Progress state for an EXPORT partition
 */
public class ExportProgressState {

    @JsonProperty("snapshotId")
    private String snapshotId;

    @JsonProperty("exportTaskId")
    private String exportTaskId;

    @JsonProperty("iamRoleArn")
    private String iamRoleArn;

    @JsonProperty("bucket")
    private String bucket;

    @JsonProperty("prefix")
    private String prefix;

    @JsonProperty("tables")
    private List<String> tables;

    @JsonProperty("kmsKeyId")
    private String kmsKeyId;

    @JsonProperty("exportTime")
    private String exportTime;

    @JsonProperty("status")
    private String status;

    public String getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String getExportTaskId() {
        return exportTaskId;
    }

    public void setExportTaskId(String exportTaskId) {
        this.exportTaskId = exportTaskId;
    }

    public String getIamRoleArn() {
        return iamRoleArn;
    }

    public void setIamRoleArn(String iamRoleArn) {
        this.iamRoleArn = iamRoleArn;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public void setKmsKeyId(String kmsKeyId) {
        this.kmsKeyId = kmsKeyId;
    }

    public String getExportTime() {
        return exportTime;
    }

    public void setExportTime(String exportTime) {
        this.exportTime = exportTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
