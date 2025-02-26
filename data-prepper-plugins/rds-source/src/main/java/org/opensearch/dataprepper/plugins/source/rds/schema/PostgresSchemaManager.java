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

import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;


public class PostgresSchemaManager implements SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaManager.class);
    private final ConnectionManager connectionManager;

    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String TYPE_NAME = "TYPE_NAME";
    static final String PGOUTPUT = "pgoutput";
    static final String DROP_PUBLICATION_SQL = "DROP PUBLICATION IF EXISTS ";
    static final String DROP_SLOT_SQL = "SELECT pg_drop_replication_slot(?)";

    public PostgresSchemaManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void createLogicalReplicationSlot(final List<String> tableNames, final String publicationName, final String slotName) {
        StringBuilder createPublicationStatementBuilder = new StringBuilder("CREATE PUBLICATION ")
                .append(publicationName)
                .append(" FOR TABLE ");
        for (int i = 0; i < tableNames.size(); i++) {
            createPublicationStatementBuilder.append(tableNames.get(i));
            if (i < tableNames.size() - 1) {
                createPublicationStatementBuilder.append(", ");
            }
        }
        createPublicationStatementBuilder.append(";");
        final String createPublicationStatement = createPublicationStatementBuilder.toString();

        try (Connection conn = connectionManager.getConnection()) {
            try {
                PreparedStatement statement = conn.prepareStatement(createPublicationStatement);
                statement.executeUpdate();
            } catch (Exception e) {
                LOG.warn("Failed to create publication: {}", e.getMessage());
            }

            PGConnection pgConnection = conn.unwrap(PGConnection.class);

            // Create replication slot
            PGReplicationConnection replicationConnection = pgConnection.getReplicationAPI();
            try {
                // Check if replication slot exists
                String checkSlotQuery = "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = ?);";
                PreparedStatement checkSlotStatement = conn.prepareStatement(checkSlotQuery);
                checkSlotStatement.setString(1, slotName);
                try (ResultSet resultSet = checkSlotStatement.executeQuery()) {
                    if (resultSet.next() && resultSet.getBoolean(1)) {
                        LOG.info("Replication slot {} already exists. ", slotName);
                        return;
                    }
                }

                LOG.info("Creating replication slot {}...", slotName);
                replicationConnection.createReplicationSlot()
                        .logical()
                        .withSlotName(slotName)
                        .withOutputPlugin(PGOUTPUT)
                        .make();
                LOG.info("Replication slot {} created successfully. ", slotName);
            } catch (Exception e) {
                LOG.warn("Failed to create replication slot {}: {}", slotName, e.getMessage());
            }
        } catch (Exception e) {
            LOG.error("Exception when creating replication slot. ", e);
        }
    }

    public void deleteLogicalReplicationSlot(final String publicationName, final String slotName) {
        try (Connection conn = connectionManager.getConnection()) {
            if (slotName != null) {
                try (PreparedStatement dropSlotStatement = conn.prepareStatement(DROP_SLOT_SQL)) {
                    dropSlotStatement.setString(1, slotName);
                    dropSlotStatement.execute();
                    LOG.info("Replication slot {} dropped successfully.", slotName);
                } catch (Exception e) {
                    LOG.warn("Failed to drop replication slot: {}", e.getMessage());
                }
            }

            if (publicationName != null) {
                try (Statement dropPublicationStatement = conn.createStatement()) {
                    dropPublicationStatement.execute(DROP_PUBLICATION_SQL + publicationName);
                    LOG.info("Publication {} dropped successfully.", publicationName);
                } catch (Exception e) {
                    LOG.warn("Failed to drop publication: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.error("Exception when connecting to database to drop replication slot. ", e);
        }
    }

    @Override
    public List<String> getPrimaryKeys(final String fullTableName) {
        final String[] splits = fullTableName.split("\\.");
        final String database = splits[0];
        final String schema = splits[1];
        final String table = splits[2];
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            final List<String> primaryKeys = new ArrayList<>();
            try (final Connection connection = connectionManager.getConnection()) {
                try (final ResultSet rs = connection.getMetaData().getPrimaryKeys(database, schema, table)) {
                    while (rs.next()) {
                        primaryKeys.add(rs.getString(COLUMN_NAME));
                    }
                    if (primaryKeys.isEmpty()) {
                        throw new NoSuchElementException("No primary keys found for table " + fullTableName);
                    }
                    return primaryKeys;
                }
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("Failed to get primary keys for table {}, retrying", fullTableName, e);
            }
            applyBackoff();
            retry++;
        }
        throw new RuntimeException("Failed to get primary keys for table " + fullTableName);
    }


    public Map<String, String> getColumnDataTypes(final String fullTableName) {
        final String[] splits = fullTableName.split("\\.");
        final String database = splits[0];
        final String schema = splits[1];
        final String table = splits[2];
        final Map<String, String> columnsToDataType =  new HashMap<>();
        for (int retry = 0; retry <= NUM_OF_RETRIES; retry++) {
            try (Connection connection = connectionManager.getConnection()) {
                final DatabaseMetaData metaData = connection.getMetaData();

                // Get all enum columns for this table
                Set<String> enumColumns = getEnumColumns(fullTableName);

                // Retrieve column metadata
                try (ResultSet columns = metaData.getColumns(database, schema, table, null)) {
                    while (columns.next()) {
                        String columnName = columns.getString(COLUMN_NAME);
                        String typeName = columns.getString(TYPE_NAME);
                        if (enumColumns.contains(columnName)) {
                           columnsToDataType.put(columnName, "enum");
                        }
                        else {
                            columnsToDataType.put(columnName, typeName);
                        }
                    }
                }
                return columnsToDataType;
            } catch (final Exception e) {
                LOG.error("Failed to get dataTypes for database {} schema {} table {}, retrying", database, schema, table, e);
                if (retry == NUM_OF_RETRIES) {
                    throw new RuntimeException(String.format("Failed to get dataTypes for database %s schema %s table %s after " +
                            "%d retries", database, schema, table, retry), e);
                }
            }
            applyBackoff();
        }
        return columnsToDataType;
    }

    public Set<String> getEnumColumns(final String fullTableName) {
        final String[] splits = fullTableName.split("\\.");
        final String database = splits[0];
        final String schema = splits[1];
        final String table = splits[2];
        final Set<String> enumColumns = new HashSet<>();
        for (int retry = 0; retry <= NUM_OF_RETRIES; retry++) {
            try (Connection connection = connectionManager.getConnection()) {
                String getEnumColumnsQuery = "SELECT a.attname AS column_name " +
                        "FROM pg_database d " +
                        "JOIN pg_namespace n ON n.nspname = ? " +
                        "JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = ? " +
                        "JOIN pg_attribute a ON a.attrelid = c.oid " +
                        "JOIN pg_type t ON t.oid = a.atttypid " +
                        "WHERE d.datname = ? " +
                        "AND t.typtype = 'e'";

                try (PreparedStatement getEnumStatement = connection.prepareStatement(getEnumColumnsQuery)) {
                    getEnumStatement.setString(1, schema);
                    getEnumStatement.setString(2, table);
                    getEnumStatement.setString(3, database);

                    try (ResultSet rs = getEnumStatement.executeQuery()) {
                        while (rs.next()) {
                            enumColumns.add(rs.getString("column_name"));
                        }
                    }
                }
                return enumColumns;
            } catch (final Exception e) {
                LOG.error("Failed to get enum columns for database {} schema {} table {}, retrying", database, schema, table, e);
                if (retry == NUM_OF_RETRIES) {
                    throw new RuntimeException(String.format("Failed to get enum columns for database %s schema %s table %s after " +
                            "%d retries", database, schema, table, retry), e);
                }
            }
            applyBackoff();
        }
        return enumColumns;
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
