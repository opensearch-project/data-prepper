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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class QueryManagerTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Connection connection;

    @Mock
    private ResultSet resultSet;

    private QueryManager queryManager;

    @BeforeEach
    void setUp() {
        queryManager = createObjectUnderTest();
    }

    @Test
    void test_selectRows_returns_expected_list() throws SQLException {
        final Statement statement = mock(Statement.class);
        final String query = UUID.randomUUID().toString();
        final ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        final String column1 = UUID.randomUUID().toString();
        final Object value1 = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(query)).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        when(resultSetMetaData.getColumnName(1)).thenReturn(column1);
        when(resultSet.getObject(1)).thenReturn(value1);

        final List<Map<String, Object>> result = queryManager.selectRows(query);

        assertThat(result.size(), is(1));
        final Map<String, Object> row = result.get(0);
        assertThat(row.size(), is(1));
        assertThat(row.get(column1), is(value1));
    }

    @Test
    void test_selectRows_returns_empty_list_when_exception_occurs() throws SQLException {
        final String query = UUID.randomUUID().toString();
        when(connectionManager.getConnection()).thenThrow(new RuntimeException());

        final List<Map<String, Object>> result = queryManager.selectRows(query);

        assertThat(result.size(), is(0));
    }

    private QueryManager createObjectUnderTest() {
        return new QueryManager(connectionManager);
    }
}