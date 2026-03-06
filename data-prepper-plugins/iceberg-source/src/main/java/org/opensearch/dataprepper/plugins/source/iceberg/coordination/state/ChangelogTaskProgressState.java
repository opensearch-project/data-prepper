/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ChangelogTaskProgressState {

    @JsonProperty("snapshotId")
    private long snapshotId;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("loadedRecords")
    private long loadedRecords;

    @JsonProperty("totalRecords")
    private long totalRecords;

    @JsonProperty("dataFilePaths")
    private List<String> dataFilePaths;

    @JsonProperty("taskTypes")
    private List<String> taskTypes;

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

    public long getLoadedRecords() {
        return loadedRecords;
    }

    public void setLoadedRecords(final long loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(final long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public List<String> getDataFilePaths() {
        return dataFilePaths;
    }

    public void setDataFilePaths(final List<String> dataFilePaths) {
        this.dataFilePaths = dataFilePaths;
    }

    public List<String> getTaskTypes() {
        return taskTypes;
    }

    public void setTaskTypes(final List<String> taskTypes) {
        this.taskTypes = taskTypes;
    }
}
