/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyAction;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MySqlSchemaManager implements SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSchemaManager.class);

    static final String[] TABLE_TYPES = new String[]{"TABLE"};
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String BINLOG_STATUS_QUERY = "SHOW MASTER STATUS";
    static final String BINLOG_FILE = "File";
    static final String BINLOG_POSITION = "Position";
    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;
    static final String TYPE_NAME = "TYPE_NAME";
    static final String FKTABLE_NAME = "FKTABLE_NAME";
    static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
    static final String PKTABLE_NAME = "PKTABLE_NAME";
    static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
    static final String UPDATE_RULE = "UPDATE_RULE";
    static final String DELETE_RULE = "DELETE_RULE";
    static final String COLUMN_DEF = "COLUMN_DEF";
    private final ConnectionManager connectionManager;

    public MySqlSchemaManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public List<String> getPrimaryKeys(final String fullTableName) {
        final String database = fullTableName.split("\\.")[0];
        final String table = fullTableName.split("\\.")[1];
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            final List<String> primaryKeys = new ArrayList<>();
            try (final Connection connection = connectionManager.getConnection()) {
                try (final ResultSet rs = connection.getMetaData().getPrimaryKeys(database, null, table)) {
                    while (rs.next()) {
                        primaryKeys.add(rs.getString(COLUMN_NAME));
                    }
                    return primaryKeys;
                }
            } catch (Exception e) {
                LOG.error("Failed to get primary keys for table {}, retrying", table, e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to get primary keys for table {}", table);
        return List.of();
    }

    public Map<String, String> getColumnDataTypes(final String fullTableName) {
        final String database = fullTableName.split("\\.")[0];
        final String tableName = fullTableName.split("\\.")[1];
        final Map<String, String> columnsToDataType =  new HashMap<>();
        for (int retry = 0; retry <= NUM_OF_RETRIES; retry++) {
            try (Connection connection = connectionManager.getConnection()) {
                final DatabaseMetaData metaData = connection.getMetaData();

                // Retrieve column metadata
                try (ResultSet columns = metaData.getColumns(database, null, tableName, null)) {
                    while (columns.next()) {
                        columnsToDataType.put(
                            columns.getString(COLUMN_NAME),
                            columns.getString(TYPE_NAME)
                        );
                    }
                }
                return columnsToDataType;
            } catch (final Exception e) {
                LOG.error("Failed to get dataTypes for database {} table {}, retrying", database, tableName, e);
                if (retry == NUM_OF_RETRIES) {
                    throw new RuntimeException(String.format("Failed to get dataTypes for database %s table %s after " +
                            "%d retries", database, tableName, retry), e);
                }
            }
            applyBackoff();
        }
        return columnsToDataType;
    }

    public Optional<BinlogCoordinate> getCurrentBinaryLogPosition() {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try (final Connection connection = connectionManager.getConnection()) {
                final Statement statement = connection.createStatement();
                final ResultSet rs = statement.executeQuery(BINLOG_STATUS_QUERY);
                if (rs.next()) {
                    return Optional.of(new BinlogCoordinate(rs.getString(BINLOG_FILE), rs.getLong(BINLOG_POSITION)));
                }
            } catch (Exception e) {
                LOG.error("Failed to get current binary log position, retrying", e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to get current binary log position");
        return Optional.empty();
    }

    /**
     * Get the foreign key relations associated with the given tables.
     *
     * @param tableNames the table names
     * @return the foreign key relations
     */
    public List<ForeignKeyRelation> getForeignKeyRelations(List<String> tableNames) {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try (final Connection connection = connectionManager.getConnection()) {
                final List<ForeignKeyRelation> foreignKeyRelations = new ArrayList<>();
                DatabaseMetaData metaData = connection.getMetaData();
                for (final String tableName : tableNames) {
                    String database = tableName.split("\\.")[0];
                    String table = tableName.split("\\.")[1];
                    ResultSet tableResult = metaData.getTables(database, null, table, TABLE_TYPES);
                    while (tableResult.next()) {
                        ResultSet foreignKeys = metaData.getImportedKeys(database, null, table);

                        while (foreignKeys.next()) {
                            String fkTableName = foreignKeys.getString(FKTABLE_NAME);
                            String fkColumnName = foreignKeys.getString(FKCOLUMN_NAME);
                            String pkTableName = foreignKeys.getString(PKTABLE_NAME);
                            String pkColumnName = foreignKeys.getString(PKCOLUMN_NAME);
                            ForeignKeyAction updateAction = ForeignKeyAction.getActionFromMetadata(foreignKeys.getShort(UPDATE_RULE));
                            ForeignKeyAction deleteAction = ForeignKeyAction.getActionFromMetadata(foreignKeys.getShort(DELETE_RULE));

                            Object defaultValue = null;
                            if (updateAction == ForeignKeyAction.SET_DEFAULT || deleteAction == ForeignKeyAction.SET_DEFAULT) {
                                // Get column default
                                ResultSet columnResult = metaData.getColumns(database, null, table, fkColumnName);

                                if (columnResult.next()) {
                                    defaultValue = columnResult.getObject(COLUMN_DEF);
                                }
                            }

                            ForeignKeyRelation foreignKeyRelation = ForeignKeyRelation.builder()
                                    .databaseName(database)
                                    .parentTableName(pkTableName)
                                    .referencedKeyName(pkColumnName)
                                    .childTableName(fkTableName)
                                    .foreignKeyName(fkColumnName)
                                    .foreignKeyDefaultValue(defaultValue)
                                    .updateAction(updateAction)
                                    .deleteAction(deleteAction)
                                    .build();

                            foreignKeyRelations.add(foreignKeyRelation);
                        }
                    }
                }

                return foreignKeyRelations;
            } catch (Exception e) {
                LOG.error("Failed to scan foreign key references, retrying", e);
            }
            applyBackoff();
            retry++;
        }
        LOG.warn("Failed to scan foreign key references");
        return List.of();
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
