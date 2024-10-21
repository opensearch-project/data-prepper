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
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyAction;
import org.opensearch.dataprepper.plugins.source.rds.model.ForeignKeyRelation;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_FILE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_POSITION;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.BINLOG_STATUS_QUERY;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.COLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.TYPE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.DELETE_RULE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.FKCOLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.FKTABLE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.PKCOLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.PKTABLE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager.UPDATE_RULE;

@ExtendWith(MockitoExtension.class)
class SchemaManagerTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

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

    @Test
    public void getColumnDataTypes_whenFailedToRetrieveColumns_shouldThrowException() throws SQLException {
        final String database = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(database, null, tableName, null)).thenThrow(new SQLException("Test exception"));

        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(database, tableName));
    }

    @Test
    public void getColumnDataTypes_whenFailedToGetConnection_shouldThrowException() throws SQLException {
        final String database = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenThrow(new SQLException("Connection failed"));

        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(database, tableName));
    }

    @Test
    void getColumnDataTypes_whenColumnsExist_shouldReturnValidMapping() throws SQLException {
        final String database = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final Map<String, String> expectedColumnTypes = Map.of(
                "id", "INTEGER",
                "name", "VARCHAR",
                "created_at", "TIMESTAMP"
        );

        // Setup the mocks
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(database, null, tableName, null))
                .thenReturn(resultSet);

        // Setup ResultSet to return our expected columns
        when(resultSet.next())
                .thenReturn(true, true, true, false); // Three columns, then done
        when(resultSet.getString(COLUMN_NAME))
                .thenReturn("id", "name", "created_at");
        when(resultSet.getString(TYPE_NAME))
                .thenReturn("INTEGER", "VARCHAR", "TIMESTAMP");

        Map<String, String> result = schemaManager.getColumnDataTypes(database, tableName);

        assertThat(result, notNullValue());
        assertThat(result.size(), is(expectedColumnTypes.size()));
        assertThat(result, equalTo(expectedColumnTypes));
    }

    @Test
    void test_getForeignKeyRelations_returns_foreign_key_relations() throws SQLException {
        final String databaseName = "test-db";
        final String tableName = "test-table";
        final List<String> tableNames = List.of(databaseName + "." + tableName);
        final ResultSet tableResult = mock(ResultSet.class);
        final ResultSet foreignKeys = mock(ResultSet.class);
        final String fkTableName = UUID.randomUUID().toString();
        final String fkColumnName = UUID.randomUUID().toString();
        final String pkTableName = UUID.randomUUID().toString();
        final String pkColumnName = UUID.randomUUID().toString();
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(eq(databaseName), any(), eq(tableName), any())).thenReturn(tableResult);
        when(tableResult.next()).thenReturn(true, false);
        when(metaData.getImportedKeys(eq(databaseName), any(), eq(tableName))).thenReturn(foreignKeys);
        when(foreignKeys.next()).thenReturn(true, false);
        when(foreignKeys.getString(FKTABLE_NAME)).thenReturn(fkTableName);
        when(foreignKeys.getString(FKCOLUMN_NAME)).thenReturn(fkColumnName);
        when(foreignKeys.getString(PKTABLE_NAME)).thenReturn(pkTableName);
        when(foreignKeys.getString(PKCOLUMN_NAME)).thenReturn(pkColumnName);
        when(foreignKeys.getShort(UPDATE_RULE)).thenReturn((short)DatabaseMetaData.importedKeyCascade);
        when(foreignKeys.getShort(DELETE_RULE)).thenReturn((short)DatabaseMetaData.importedKeySetNull);

        final List<ForeignKeyRelation> foreignKeyRelations = schemaManager.getForeignKeyRelations(tableNames);

        assertThat(foreignKeyRelations.size(), is(1));

        ForeignKeyRelation foreignKeyRelation = foreignKeyRelations.get(0);
        assertThat(foreignKeyRelation.getParentTableName(), is(pkTableName));
        assertThat(foreignKeyRelation.getReferencedKeyName(), is(pkColumnName));
        assertThat(foreignKeyRelation.getChildTableName(), is(fkTableName));
        assertThat(foreignKeyRelation.getForeignKeyName(), is(fkColumnName));
        assertThat(foreignKeyRelation.getUpdateAction(), is(ForeignKeyAction.CASCADE));
        assertThat(foreignKeyRelation.getDeleteAction(), is(ForeignKeyAction.SET_NULL));
    }

    private SchemaManager createObjectUnderTest() {
        return new SchemaManager(connectionManager);
    }
}