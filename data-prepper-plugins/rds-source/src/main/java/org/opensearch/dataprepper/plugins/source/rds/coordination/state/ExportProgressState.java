/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Progress state for an EXPORT partition
 */
public class ExportProgressState {

    @JsonProperty("engineType")
    private String engineType;

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

    /**
     * Map of table name to primary keys
     */
    @JsonProperty("primaryKeyMap")
    private Map<String, List<String>> primaryKeyMap;

    @JsonProperty("kmsKeyId")
    private String kmsKeyId;

    @JsonProperty("snapshotTime")
    private long snapshotTime;

    @JsonProperty("status")
    private String status;

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

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

    public Map<String, List<String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    public void setPrimaryKeyMap(Map<String, List<String>> primaryKeyMap) {
        this.primaryKeyMap = primaryKeyMap;
    }

    public String getKmsKeyId() {
        return kmsKeyId;
    }

    public void setKmsKeyId(String kmsKeyId) {
        this.kmsKeyId = kmsKeyId;
    }

    public long getSnapshotTime() {
        return snapshotTime;
    }

    public void setSnapshotTime(long snapshotTime) {
        this.snapshotTime = snapshotTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
