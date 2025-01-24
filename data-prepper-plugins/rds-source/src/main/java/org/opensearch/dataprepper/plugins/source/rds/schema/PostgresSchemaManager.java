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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class PostgresSchemaManager implements SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaManager.class);
    private final ConnectionManager connectionManager;

    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String PGOUTPUT = "pgoutput";

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
                        throw new NoSuchElementException("No primary keys found for table " + table);
                    }
                    return primaryKeys;
                }
            } catch (NoSuchElementException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("Failed to get primary keys for table {}, retrying", table, e);
            }
            applyBackoff();
            retry++;
        }
        throw new RuntimeException("Failed to get primary keys for table " + table);
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
