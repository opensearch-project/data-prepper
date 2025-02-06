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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresSchemaManagerTest {

    @Mock
    private PostgresConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

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

    private PostgresSchemaManager createObjectUnderTest() {
        return new PostgresSchemaManager(connectionManager);
    }
}
