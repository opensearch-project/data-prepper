package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BooleanTypeHandlerTest {
    private BooleanTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new BooleanTypeHandler();
    }

    @Test
    void test_handle_true_values() {
        String value = "t";
        Object result = handler.process(PostgresDataType.BOOLEAN, "testColumn", value);
        assertThat(result, is(instanceOf(Boolean.class)));
        assertThat(result, is(Boolean.TRUE));
    }

    @Test
    void test_handle_false_values() {
        String value = "f";
        Object result = handler.process(PostgresDataType.BOOLEAN, "testColumn", value);
        assertThat(result, is(instanceOf(Boolean.class)));
        assertThat(result, is(Boolean.FALSE));}

    @Test
    void test_handle_non_boolean_type() {
        assertThrows(IllegalArgumentException.class, () ->
                handler.process(PostgresDataType.INTEGER, "testColumn", 123)
        );
    }

}
