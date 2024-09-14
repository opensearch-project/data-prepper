/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.List;

public class TableMetadata {
    private String databaseName;
    private String tableName;
    private List<String> columnNames;
    private List<String> primaryKeys;

    public TableMetadata(String tableName, String databaseName, List<String> columnNames, List<String> primaryKeys) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.columnNames = columnNames;
        this.primaryKeys = primaryKeys;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() {
        return databaseName + "." + tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

}
