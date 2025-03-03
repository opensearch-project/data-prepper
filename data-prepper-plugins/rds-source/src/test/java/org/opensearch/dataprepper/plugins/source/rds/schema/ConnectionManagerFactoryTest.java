package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConnectionManagerFactoryTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private RdsSourceConfig sourceConfig;

    @Mock
    private DbMetadata dbMetadata;

    private ConnectionManagerFactory connectionManagerFactory;

    @BeforeEach
    void setUp() {
        connectionManagerFactory = createObjectUnderTest();
    }

    @Test
    void test_getConnectionManager_for_mysql() {
        when(sourceConfig.getEngine()).thenReturn(EngineType.MYSQL);
        final ConnectionManager connectionManager = connectionManagerFactory.getConnectionManager();
        assertThat(connectionManager, notNullValue());
        assertThat(connectionManager, instanceOf(MySqlConnectionManager.class));
    }

    @Test
    void test_getConnectionManager_for_postgres() {
        when(sourceConfig.getEngine()).thenReturn(EngineType.POSTGRES);
        final ConnectionManager connectionManager = connectionManagerFactory.getConnectionManager();
        assertThat(connectionManager, notNullValue());
        assertThat(connectionManager, instanceOf(PostgresConnectionManager.class));
    }

    private ConnectionManagerFactory createObjectUnderTest() {
        return new ConnectionManagerFactory(sourceConfig, dbMetadata);
    }
}
