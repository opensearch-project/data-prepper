package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class BinaryTypeHandlerTest {
    private BinaryTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new BinaryTypeHandler();
    }

    @Test
    public void test_handle_binary_string() {
        String columnName = "testColumn";
        PostgresDataType columnType = PostgresDataType.BYTEA;
        String value = "\\xDEADBEEF";
        Object result = handler.process(columnType, columnName, value);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }
}
