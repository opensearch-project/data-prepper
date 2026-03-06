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

public class InitialLoadTaskProgressState {

    @JsonProperty("snapshotId")
    private long snapshotId;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("dataFilePath")
    private String dataFilePath;

    @JsonProperty("totalRecords")
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
