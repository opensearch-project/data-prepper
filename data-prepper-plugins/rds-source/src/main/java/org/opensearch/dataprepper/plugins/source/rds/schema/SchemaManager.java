/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);
    static final String COLUMN_NAME = "COLUMN_NAME";
    static final String BINLOG_STATUS_QUERY = "SHOW MASTER STATUS";
    static final String BINLOG_FILE = "File";
    static final String BINLOG_POSITION = "Position";
    private final ConnectionManager connectionManager;

    public SchemaManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public List<String> getPrimaryKeys(final String database, final String table) {
        final List<String> primaryKeys = new ArrayList<>();
        try (final Connection connection = connectionManager.getConnection()) {
            final ResultSet rs = connection.getMetaData().getPrimaryKeys(database, null, table);
            while (rs.next()) {
                primaryKeys.add(rs.getString(COLUMN_NAME));
            }
        } catch (SQLException e) {
            LOG.error("Failed to get primary keys for table {}", table, e);
        }
        return primaryKeys;
    }

    public Optional<BinlogCoordinate> getCurrentBinaryLogPosition() {
        try (final Connection connection = connectionManager.getConnection()) {
            final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery(BINLOG_STATUS_QUERY);
            if (rs.next()) {
                return Optional.of(new BinlogCoordinate(rs.getString(BINLOG_FILE), rs.getLong(BINLOG_POSITION)));
            }
        } catch (SQLException e) {
            LOG.error("Failed to get current binary log position", e);
        }
        return Optional.empty();
    }
}
