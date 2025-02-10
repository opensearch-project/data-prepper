/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class DataFileProgressState {

    @JsonProperty("engineType")
    private String engineType;

    @JsonProperty("isLoaded")
    private boolean isLoaded = false;

    @JsonProperty("totalRecords")
    private int totalRecords;

    @JsonProperty("sourceDatabase")
    private String sourceDatabase;

    /**
     * For MySQL, sourceTable is in the format of tableName
     * For Postgres, sourceTable is in the format of schemaName.tableName
     */
    @JsonProperty("sourceTable")
    private String sourceTable;

    /**
     * Map of table name to primary keys
     */
    @JsonProperty("primaryKeyMap")
    private Map<String, List<String>> primaryKeyMap;

    @JsonProperty("snapshotTime")
    private long snapshotTime;

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public int getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }

    public boolean getLoaded() {
        return isLoaded;
    }

    public void setLoaded(boolean loaded) {
        this.isLoaded = loaded;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public void setSourceDatabase(String sourceDatabase) {
        this.sourceDatabase = sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public long getSnapshotTime() {
        return snapshotTime;
    }

    public void setSnapshotTime(long snapshotTime) {
        this.snapshotTime = snapshotTime;
    }

    public Map<String, List<String>> getPrimaryKeyMap() {
        return primaryKeyMap;
    }

    public void setPrimaryKeyMap(Map<String, List<String>> primaryKeyMap) {
        this.primaryKeyMap = primaryKeyMap;
    }
}
