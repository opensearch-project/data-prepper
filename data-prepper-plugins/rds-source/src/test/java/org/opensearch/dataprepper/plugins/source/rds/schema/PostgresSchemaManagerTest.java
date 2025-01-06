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
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
    void test_createLogicalReplicationSlot() throws SQLException {
        final List<String> tableNames = List.of("table1", "table2");
        final String publicationName = "publication1";
        final String slotName = "slot1";
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        final PGConnection pgConnection = mock(PGConnection.class);
        final PGReplicationConnection replicationConnection = mock(PGReplicationConnection.class);
        final ChainedCreateReplicationSlotBuilder chainedCreateSlotBuilder = mock(ChainedCreateReplicationSlotBuilder.class);
        final ChainedLogicalCreateSlotBuilder slotBuilder = mock(ChainedLogicalCreateSlotBuilder.class);

        ArgumentCaptor<String> statementCaptor = ArgumentCaptor.forClass(String.class);

        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(statementCaptor.capture())).thenReturn(preparedStatement);
        when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
        when(pgConnection.getReplicationAPI()).thenReturn(replicationConnection);
        when(replicationConnection.createReplicationSlot()).thenReturn(chainedCreateSlotBuilder);
        when(chainedCreateSlotBuilder.logical()).thenReturn(slotBuilder);
        when(slotBuilder.withSlotName(anyString())).thenReturn(slotBuilder);
        when(slotBuilder.withOutputPlugin(anyString())).thenReturn(slotBuilder);

        schemaManager.createLogicalReplicationSlot(tableNames, publicationName, slotName);

        String statement = statementCaptor.getValue();
        assertThat(statement, is("CREATE PUBLICATION " + publicationName + " FOR TABLE " + String.join(", ", tableNames) + ";"));
        verify(preparedStatement).executeUpdate();
        verify(pgConnection).getReplicationAPI();
        verify(replicationConnection).createReplicationSlot();
        verify(chainedCreateSlotBuilder).logical();
        verify(slotBuilder).withSlotName(slotName);
        verify(slotBuilder).withOutputPlugin("pgoutput");
        verify(slotBuilder).make();
    }

    private PostgresSchemaManager createObjectUnderTest() {
        return new PostgresSchemaManager(connectionManager);
    }
}
