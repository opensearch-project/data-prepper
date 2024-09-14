/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_FILE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_POSITION;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_STATUS_QUERY;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.COLUMN_NAME;

@ExtendWith(MockitoExtension.class)
class SchemaManagerTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private ResultSet resultSet;

    private SchemaManager schemaManager;

    @BeforeEach
    void setUp() {
        schemaManager = createObjectUnderTest();
    }

    @Test
    void test_getPrimaryKeys_returns_primary_keys() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final String primaryKey = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData().getPrimaryKeys(databaseName, null, tableName)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(COLUMN_NAME)).thenReturn(primaryKey);

        final List<String> primaryKeys = schemaManager.getPrimaryKeys(databaseName, tableName);

        assertThat(primaryKeys, contains(primaryKey));
    }

    @Test
    void test_getPrimaryKeys_throws_exception_then_returns_empty_list() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenThrow(SQLException.class);

        final List<String> primaryKeys = schemaManager.getPrimaryKeys(databaseName, tableName);

        assertThat(primaryKeys, empty());
    }

    @Test
    void test_getCurrentBinaryLogPosition_returns_binlog_coords() throws SQLException {
        final Statement statement = mock(Statement.class);
        final String binlogFile = UUID.randomUUID().toString();
        final long binlogPosition = 123L;
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(BINLOG_STATUS_QUERY)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(BINLOG_FILE)).thenReturn(binlogFile);
        when(resultSet.getLong(BINLOG_POSITION)).thenReturn(binlogPosition);

        final Optional<BinlogCoordinate> binlogCoordinate = schemaManager.getCurrentBinaryLogPosition();

        assertThat(binlogCoordinate.isPresent(), is(true));
        assertThat(binlogCoordinate.get().getBinlogFilename(), is(binlogFile));
        assertThat(binlogCoordinate.get().getBinlogPosition(), is(binlogPosition));
    }

    @Test
    void test_getCurrentBinaryLogPosition_throws_exception_then_returns_empty() throws SQLException {
        when(connectionManager.getConnection()).thenThrow(SQLException.class);

        final Optional<BinlogCoordinate> binlogCoordinate = schemaManager.getCurrentBinaryLogPosition();

        assertThat(binlogCoordinate.isPresent(), is(false));
    }

    private SchemaManager createObjectUnderTest() {
        return new SchemaManager(connectionManager);
    }
}