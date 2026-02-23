/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InitialLoadTaskProgressState {

    @JsonProperty("snapshot_id")
    private long snapshotId;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("data_file_path")
    private String dataFilePath;

    @JsonProperty("total_records")
    private long totalRecords;

    public long getSnapshotId() {
        return snapshotId;
    }

    public void setSnapshotId(final long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public void setDataFilePath(final String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(final long totalRecords) {
        this.totalRecords = totalRecords;
    }
}
