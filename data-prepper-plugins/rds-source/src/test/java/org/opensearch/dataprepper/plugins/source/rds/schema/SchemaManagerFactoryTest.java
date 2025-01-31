package org.opensearch.dataprepper.plugins.source.rds.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;


@ExtendWith(MockitoExtension.class)
class SchemaManagerFactoryTest {

    @Mock
    private MySqlConnectionManager mySqlConnectionManager;

    @Mock
    private PostgresConnectionManager postgresConnectionManager;

    private SchemaManagerFactory schemaManagerFactory;
    private ConnectionManager connectionManager;

    @BeforeEach
    void setUp() {
    }

    @Test
    void test_getSchemaManager_for_mysql() {
        connectionManager = mySqlConnectionManager;
        schemaManagerFactory = createObjectUnderTest();

        assertThat(schemaManagerFactory.getSchemaManager(), instanceOf(MySqlSchemaManager.class));
    }

    @Test
    void test_getSchemaManager_for_postgres() {
        connectionManager = postgresConnectionManager;
        schemaManagerFactory = createObjectUnderTest();

        assertThat(schemaManagerFactory.getSchemaManager(), instanceOf(PostgresSchemaManager.class));
    }

    private SchemaManagerFactory createObjectUnderTest() {
        return new SchemaManagerFactory(connectionManager);
    }
}
