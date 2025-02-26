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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.fluent.ChainedCreateReplicationSlotBuilder;
import org.postgresql.replication.fluent.logical.ChainedLogicalCreateSlotBuilder;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.COLUMN_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager.TYPE_NAME;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresSchemaManager.DROP_PUBLICATION_SQL;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresSchemaManager.DROP_SLOT_SQL;

@ExtendWith(MockitoExtension.class)
class PostgresSchemaManagerTest {

    @Mock
    private PostgresConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private DatabaseMetaData databaseMetaData;

    @Mock
    private ResultSet resultSet;

    @Mock
    private PreparedStatement preparedStatement;

    private PostgresSchemaManager schemaManager;

    @BeforeEach
    void setUp() {
        schemaManager = createObjectUnderTest();
    }

    @Test
    void test_createLogicalReplicationSlot_creates_slot_if_not_exists() throws SQLException {
        final List<String> tableNames = List.of("table1", "table2");
        final String publicationName = "publication1";
        final String slotName = "slot1";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final PGConnection pgConnection = mock(PGConnection.class);
        final PGReplicationConnection replicationConnection = mock(PGReplicationConnection.class);
        final ResultSet resultSet = mock(ResultSet.class);
        final ChainedCreateReplicationSlotBuilder chainedCreateSlotBuilder = mock(ChainedCreateReplicationSlotBuilder.class);
        final ChainedLogicalCreateSlotBuilder slotBuilder = mock(ChainedLogicalCreateSlotBuilder.class);

        ArgumentCaptor<String> statementCaptor = ArgumentCaptor.forClass(String.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(statementCaptor.capture())).thenReturn(preparedStatement);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);  // Replication slot doesn't exist
        when(pgConnection.getReplicationAPI()).thenReturn(replicationConnection);
        when(replicationConnection.createReplicationSlot()).thenReturn(chainedCreateSlotBuilder);
        when(chainedCreateSlotBuilder.logical()).thenReturn(slotBuilder);
        when(slotBuilder.withSlotName(anyString())).thenReturn(slotBuilder);
        when(slotBuilder.withOutputPlugin(anyString())).thenReturn(slotBuilder);

        schemaManager.createLogicalReplicationSlot(tableNames, publicationName, slotName);

        List<String> statements = statementCaptor.getAllValues();
        assertThat(statements.get(0), is("CREATE PUBLICATION " + publicationName + " FOR TABLE " + String.join(", ", tableNames) + ";"));
        assertThat(statements.get(1), is("SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = ?);"));
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).executeQuery();
        verify(pgConnection).getReplicationAPI();
        verify(replicationConnection).createReplicationSlot();
        verify(chainedCreateSlotBuilder).logical();
        verify(slotBuilder).withSlotName(slotName);
        verify(slotBuilder).withOutputPlugin("pgoutput");
        verify(slotBuilder).make();
    }

    @Test
    void test_createLogicalReplicationSlot_skip_creation_if_slot_exists() throws SQLException {
        final List<String> tableNames = List.of("table1", "table2");
        final String publicationName = "publication1";
        final String slotName = "slot1";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final PGConnection pgConnection = mock(PGConnection.class);
        final PGReplicationConnection replicationConnection = mock(PGReplicationConnection.class);
        final ResultSet resultSet = mock(ResultSet.class);

        ArgumentCaptor<String> statementCaptor = ArgumentCaptor.forClass(String.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(statementCaptor.capture())).thenReturn(preparedStatement);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);  // Replication slot exists
        when(resultSet.getBoolean(1)).thenReturn(true);
        when(pgConnection.getReplicationAPI()).thenReturn(replicationConnection);

        schemaManager.createLogicalReplicationSlot(tableNames, publicationName, slotName);

        List<String> statements = statementCaptor.getAllValues();
        assertThat(statements.get(0), is("CREATE PUBLICATION " + publicationName + " FOR TABLE " + String.join(", ", tableNames) + ";"));
        assertThat(statements.get(1), is("SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = ?);"));
        verify(preparedStatement).executeUpdate();
        verify(preparedStatement).executeQuery();
        verify(pgConnection).getReplicationAPI();
        verify(replicationConnection, never()).createReplicationSlot();
    }

    @Test
    void test_deleteLogicalReplicationSlot_success() throws SQLException {
        final String publicationName = UUID.randomUUID().toString();
        final String slotName = UUID.randomUUID().toString();
        final PreparedStatement dropSlotStatement = mock(PreparedStatement.class);
        final Statement dropPublicationStatement = mock(Statement.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(DROP_SLOT_SQL)).thenReturn(dropSlotStatement);
        when(connection.createStatement()).thenReturn(dropPublicationStatement);

        schemaManager.deleteLogicalReplicationSlot(publicationName, slotName);

        verify(dropSlotStatement).setString(1, slotName);
        verify(dropSlotStatement).execute();
        verify(dropPublicationStatement).execute(DROP_PUBLICATION_SQL + publicationName);
    }

    @Test
    void test_getPrimaryKeys_returns_primary_keys() throws SQLException {
        final String database = UUID.randomUUID().toString();
        final String schema = UUID.randomUUID().toString();
        final String table = UUID.randomUUID().toString();
        final String fullTableName = database + "." + schema + "." + table;
        final ResultSet resultSet = mock(ResultSet.class);
        final String primaryKeyName = UUID.randomUUID().toString();

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData().getPrimaryKeys(database, schema, table)).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("COLUMN_NAME")).thenReturn(primaryKeyName);

        final List<String> primaryKeys = schemaManager.getPrimaryKeys(fullTableName);

        assertThat(primaryKeys.size(), is(1));
        assertThat(primaryKeys.get(0), is(primaryKeyName));
    }

    @Test
    void test_getPrimaryKeys_throws_exception_if_failed() throws SQLException {
        final String database = UUID.randomUUID().toString();
        final String schema = UUID.randomUUID().toString();
        final String table = UUID.randomUUID().toString();
        final String fullTableName = database + "." + schema + "." + table;
        final ResultSet resultSet = mock(ResultSet.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData().getPrimaryKeys(database, schema, table)).thenReturn(resultSet);
        when(resultSet.next()).thenThrow(RuntimeException.class);

        assertThrows(RuntimeException.class, () -> schemaManager.getPrimaryKeys(fullTableName));
    }

    @Test
    public void getColumnDataTypes_whenFailedToRetrieveColumns_shouldThrowException() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(database, schema, tableName, null)).thenThrow(new SQLException("Test exception"));

        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(fullTableName));
    }

    @Test
    public void getColumnDataTypes_whenFailedToGetConnection_shouldThrowException() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;
        when(connectionManager.getConnection()).thenThrow(new SQLException("Connection failed"));

        assertThrows(RuntimeException.class, () -> schemaManager.getColumnDataTypes(fullTableName));
    }

    @Test
    void getColumnDataTypes_whenColumnsExist_shouldReturnValidMapping() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;
        final Map<String, String> expectedColumnTypes = Map.of(
                "id", "serial",
                "name", "char",
                "created_at", "timestamp"
        );

        // Setup the mocks
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(database, schema, tableName, null))
                .thenReturn(resultSet);

        // Setup ResultSet to return our expected columns
        when(resultSet.next())
                .thenReturn(true, true, true, false); // Three columns, then done
        when(resultSet.getString(COLUMN_NAME))
                .thenReturn("id", "name", "created_at");
        when(resultSet.getString(TYPE_NAME))
                .thenReturn("serial", "char", "timestamp");

        Map<String, String> result = schemaManager.getColumnDataTypes(fullTableName);

        assertThat(result, notNullValue());
        assertThat(result.size(), is(expectedColumnTypes.size()));
        assertThat(result, equalTo(expectedColumnTypes));
    }

    @Test
    void getColumnDataTypes_whenEnumColumnsExist_shouldReturnCorrectMapping() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;

        // Setup expected data
        final Map<String, String> expectedColumnTypes = Map.of(
                "id", "serial",
                "status", "enum",  // enum column
                "name", "varchar",
                "category", "enum" // enum column
        );

        Set<String> enumColumns = Set.of("status", "category");
        PostgresSchemaManager spySchemaManager = spy(schemaManager);
        doReturn(enumColumns).when(spySchemaManager).getEnumColumns(fullTableName);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(databaseMetaData);
        when(databaseMetaData.getColumns(database, schema, tableName, null))
                .thenReturn(resultSet);

        when(resultSet.next())
                .thenReturn(true, true, true, true, false);
        when(resultSet.getString(COLUMN_NAME))
                .thenReturn("id", "status", "name", "category");
        when(resultSet.getString(TYPE_NAME))
                .thenReturn("serial", "user_status_enum", "varchar", "category_enum");

        // Execute the method
        Map<String, String> result = spySchemaManager.getColumnDataTypes(fullTableName);

        // Verify results
        assertThat(result, notNullValue());
        assertThat(result.size(), is(expectedColumnTypes.size()));
        assertThat(result, equalTo(expectedColumnTypes));

        // Verify the interactions
        verify(spySchemaManager).getEnumColumns(fullTableName);
        verify(databaseMetaData).getColumns(database, schema, tableName, null);
    }

    @Test
    void getEnumColumns_successfully_returns_enum_columns() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;
        final Set<String> expectedEnumColumns = Set.of("status", "category");

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("column_name")).thenReturn("status", "category");

        final Set<String> actualEnumColumns = schemaManager.getEnumColumns(fullTableName);

        assertThat(actualEnumColumns, equalTo(expectedEnumColumns));

        verify(preparedStatement).setString(1, schema);
        verify(preparedStatement).setString(2, tableName);
        verify(preparedStatement).setString(3, database);
    }

    @Test
    void getEnumColumns_returns_empty_set_when_no_enum_columns() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        final Set<String> actualEnumColumns = schemaManager.getEnumColumns(fullTableName);

        assertTrue(actualEnumColumns.isEmpty());
        verify(preparedStatement).setString(1, schema);
        verify(preparedStatement).setString(2, tableName);
        verify(preparedStatement).setString(3, database);
    }

    @Test
    void getEnumColumns_handles_sql_exception_with_retry() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString()))
                .thenThrow(new SQLException("Database error"))
                .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getString("column_name")).thenReturn("status");

        final Set<String> actualEnumColumns = schemaManager.getEnumColumns(fullTableName);

        assertThat(actualEnumColumns, equalTo(Set.of("status")));
        verify(preparedStatement).setString(1, schema);
        verify(preparedStatement).setString(2, tableName);
        verify(preparedStatement).setString(3, database);
    }

    @Test
    void getEnumColumns_throws_exception_after_max_retries() throws SQLException {
        final String database = "my_db";
        final String schema = "public";
        final String tableName = "test";
        final String fullTableName = database + "." + schema + "." + tableName;

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString()))
                .thenThrow(new SQLException("Database error"));

        assertThrows(RuntimeException.class, () ->
                schemaManager.getEnumColumns(fullTableName));
    }


    private PostgresSchemaManager createObjectUnderTest() {
        return new PostgresSchemaManager(connectionManager);
    }
}
