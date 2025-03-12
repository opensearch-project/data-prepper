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
import org.opensearch.dataprepper.plugins.source.rds.exception.SqlMetadataException;
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
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata.DOT_DELIMITER;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.BINLOG_FILE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.BINLOG_POSITION;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.BINLOG_STATUS_QUERY;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.COLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.TABLE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.TYPE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.DELETE_RULE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.FKCOLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.FKTABLE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.PKCOLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.PKTABLE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.UPDATE_RULE;

@ExtendWith(MockitoExtension.class)
class MySqlSchemaManagerTest {

    @Mock
    private MySqlConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private ResultSet resultSet;

    private MySqlSchemaManager schemaManager;

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

        final String fullTableName = databaseName + "." + tableName;
        final Map<String, List<String>> primaryKeysMap = schemaManager.getPrimaryKeys(List.of(fullTableName));

        assertThat(primaryKeysMap.get(fullTableName), is(List.of(primaryKey)));
    }

    @Test
    void test_getPrimaryKeys_when_connection_fails_then_throws() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenThrow(SQLException.class);

        final String fullTableName = databaseName + "." + tableName;
        assertThrows(RuntimeException.class, () -> schemaManager.getPrimaryKeys(List.of(fullTableName)));
    }

    @Test
    void test_getPrimaryKeys_when_fails_to_get_metadata_then_throws() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenThrow(SqlMetadataException.class);

        final String fullTableName = databaseName + "." + tableName;
        assertThrows(RuntimeException.class, () -> schemaManager.getPrimaryKeys(List.of(fullTableName)));
    }

    @Test
    void test_getTableNames_returns_correct_result() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final String fullTableName = databaseName + DOT_DELIMITER + tableName;
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData().getTables(databaseName, null, null, new String[]{"TABLE"})).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString(TABLE_NAME)).thenReturn(tableName);

        Set<String> tableNames = schemaManager.getTableNames(databaseName);

        assertThat(tableNames, is(Set.of(fullTableName)));
    }

    @Test
    void test_getTableNames_when_exception_then_throws() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenThrow(SQLException.class);

        assertThrows(RuntimeException.class, () -> schemaManager.getTableNames(databaseName));
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
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(databaseName, null, tableName, null)).thenThrow(new SQLException("Test exception"));

        final String fullTableName = databaseName + "." + tableName;
        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(List.of(fullTableName)));
    }

    @Test
    public void getColumnDataTypes_whenFailedToGetConnection_shouldThrowException() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenThrow(new SQLException("Connection failed"));
        final String fullTableName = databaseName + "." + tableName;
        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(List.of(fullTableName)));
    }

    @Test
    public void getColumnDataTypes_whenFailsToGetMetadata_shouldThrowException() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenThrow(SqlMetadataException.class);
        final String fullTableName = databaseName + "." + tableName;
        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(List.of(fullTableName)));
    }

    @Test
    void getColumnDataTypes_whenColumnsExist_shouldReturnValidMapping() throws SQLException {
        final String databaseName = UUID.randomUUID().toString();
        final String tableName = UUID.randomUUID().toString();
        final Map<String, String> expectedColumnTypes = Map.of(
                "id", "INTEGER",
                "name", "VARCHAR",
                "created_at", "TIMESTAMP"
        );

        // Setup the mocks
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(databaseName, null, tableName, null))
                .thenReturn(resultSet);

        // Setup ResultSet to return our expected columns
        when(resultSet.next())
                .thenReturn(true, true, true, false); // Three columns, then done
        when(resultSet.getString(COLUMN_NAME))
                .thenReturn("id", "name", "created_at");
        when(resultSet.getString(TYPE_NAME))
                .thenReturn("INTEGER", "VARCHAR", "TIMESTAMP");

        final String fullTableName = databaseName + "." + tableName;
        Map<String, Map<String, String>> result = schemaManager.getColumnDataTypes(List.of(fullTableName));

        assertThat(result, notNullValue());
        assertThat(result.size(), is(1));
        final Map<String, String> resultItem = result.get(fullTableName);
        assertThat(resultItem.size(), is(expectedColumnTypes.size()));
        assertThat(resultItem, equalTo(expectedColumnTypes));
    }

    @Test
    void test_getForeignKeyRelations_returns_foreign_key_relations() throws SQLException {
        final String databaseName = "test-db";
        final String tableName = "test-table";
        final List<String> tableNames = List.of(databaseName + DOT_DELIMITER + tableName);
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

    private MySqlSchemaManager createObjectUnderTest() {
        return new MySqlSchemaManager(connectionManager);
    }
}