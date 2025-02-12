/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TableMetadata {
    public static final String DOT_DELIMITER = ".";

    private final String databaseName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final List<String> primaryKeys;
    private final Map<String, String[]> setStrValues;
    private final Map<String, String[]> enumStrValues;

    private TableMetadata(final Builder builder) {
        this.databaseName = builder.databaseName;
        this.tableName = builder.tableName;
        this.columnNames = builder.columnNames;
        this.columnTypes = builder.columnTypes;
        this.primaryKeys = builder.primaryKeys;
        this.setStrValues = builder.setStrValues;
        this.enumStrValues = builder.enumStrValues;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullTableName() {
        return databaseName + DOT_DELIMITER + tableName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String[]> getSetStrValues() {
        return setStrValues;
    }

    public Map<String, String[]> getEnumStrValues() {
        return enumStrValues;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String databaseName;
        private String tableName;
        private List<String> columnNames = Collections.emptyList();
        private List<String> columnTypes = Collections.emptyList();
        private List<String> primaryKeys = Collections.emptyList();
        private Map<String, String[]> setStrValues = Collections.emptyMap();
        private Map<String, String[]> enumStrValues = Collections.emptyMap();

        private Builder() {
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder withColumnTypes(List<String> columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public Builder withPrimaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }

        public Builder withSetStrValues(Map<String, String[]> setStrValues) {
            this.setStrValues = setStrValues;
            return this;
        }

        public Builder withEnumStrValues(Map<String, String[]> enumStrValues) {
            this.enumStrValues = enumStrValues;
            return this;
        }

        public TableMetadata build() {
            return new TableMetadata(this);
        }
    }
}
