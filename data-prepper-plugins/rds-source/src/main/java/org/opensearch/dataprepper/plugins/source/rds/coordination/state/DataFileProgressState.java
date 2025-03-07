/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;

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
     * For PostgreSQL engine type, sourceSchema is the schema name.
     * For MySQL engine type, this field will store database name, same as sourceDatabase field.
     */
    @JsonProperty("sourceSchema")
    private String sourceSchema;

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

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    @JsonIgnore
    public String getFullSourceTableName() {
        if (EngineType.fromString(engineType).isMySql()) {
            return sourceDatabase + "." + sourceTable;
        } else if (EngineType.fromString(engineType).isPostgres()) {
            return sourceDatabase + "." + sourceSchema + "." + sourceTable;
        } else {
            throw new RuntimeException("Unsupported engine type: " + engineType);
        }
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
