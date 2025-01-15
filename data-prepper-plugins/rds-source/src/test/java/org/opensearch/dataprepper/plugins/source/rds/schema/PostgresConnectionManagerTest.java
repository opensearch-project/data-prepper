package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.DATABASE_REPLICATION;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.FALSE_VALUE;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.REQUIRE_SSL;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.SERVER_VERSION_9_4;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.SIMPLE_QUERY;
import static org.opensearch.dataprepper.plugins.source.rds.schema.PostgresConnectionManager.TRUE_VALUE;

class PostgresConnectionManagerTest {

    private String endpoint;
    private int port;
    private String username;
    private String password;
    private boolean requireSSL;
    private String database;
    private final Random random = new Random();

    @BeforeEach
    void setUp() {
        endpoint = UUID.randomUUID().toString();
        port = random.nextInt(65536);
        username = UUID.randomUUID().toString();
        password = UUID.randomUUID().toString();
    }

    @Test
    void test_getConnection_when_requireSSL_is_true() throws SQLException {
        requireSSL = true;
        final PostgresConnectionManager connectionManager = spy(createObjectUnderTest());
        final ArgumentCaptor<String> jdbcUrlArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
        doReturn(mock(Connection.class)).when(connectionManager).doGetConnection(jdbcUrlArgumentCaptor.capture(), propertiesArgumentCaptor.capture());

        connectionManager.getConnection();

        assertThat(jdbcUrlArgumentCaptor.getValue(), is(String.format(PostgresConnectionManager.JDBC_URL_FORMAT, endpoint, port, database)));
        final Properties properties = propertiesArgumentCaptor.getValue();
        assertThat(PGProperty.USER.getOrDefault(properties), is(username));
        assertThat(PGProperty.PASSWORD.getOrDefault(properties), is(password));
        assertThat(PGProperty.ASSUME_MIN_SERVER_VERSION.getOrDefault(properties), is(SERVER_VERSION_9_4));
        assertThat(PGProperty.REPLICATION.getOrDefault(properties), is(DATABASE_REPLICATION));
        assertThat(PGProperty.PREFER_QUERY_MODE.getOrDefault(properties), is(SIMPLE_QUERY));
        assertThat(PGProperty.SSL.getOrDefault(properties), is(TRUE_VALUE));
        assertThat(PGProperty.SSL_MODE.getOrDefault(properties), is(REQUIRE_SSL));
    }

    @Test
    void test_getConnection_when_requireSSL_is_false() throws SQLException {
        requireSSL = false;
        final PostgresConnectionManager connectionManager = spy(createObjectUnderTest());
        final ArgumentCaptor<String> jdbcUrlArgumentCaptor = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<Properties> propertiesArgumentCaptor = ArgumentCaptor.forClass(Properties.class);
        doReturn(mock(Connection.class)).when(connectionManager).doGetConnection(jdbcUrlArgumentCaptor.capture(), propertiesArgumentCaptor.capture());

        connectionManager.getConnection();

        assertThat(jdbcUrlArgumentCaptor.getValue(), is(String.format(PostgresConnectionManager.JDBC_URL_FORMAT, endpoint, port, database)));
        final Properties properties = propertiesArgumentCaptor.getValue();
        assertThat(PGProperty.USER.getOrDefault(properties), is(username));
        assertThat(PGProperty.PASSWORD.getOrDefault(properties), is(password));
        assertThat(PGProperty.ASSUME_MIN_SERVER_VERSION.getOrDefault(properties), is(SERVER_VERSION_9_4));
        assertThat(PGProperty.REPLICATION.getOrDefault(properties), is(DATABASE_REPLICATION));
        assertThat(PGProperty.PREFER_QUERY_MODE.getOrDefault(properties), is(SIMPLE_QUERY));
        assertThat(PGProperty.SSL.getOrDefault(properties), is(FALSE_VALUE));
    }

    private PostgresConnectionManager createObjectUnderTest() {
        return new PostgresConnectionManager(endpoint, port, username, password, requireSSL, database);
    }
}