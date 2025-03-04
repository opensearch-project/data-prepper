package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class
JsonTypeHandlerTest {
    private JsonTypeHandler handler;

    private static Stream<Arguments> provideJsonArrayData() {
        return Stream.of(
                // JSON array cases
                Arguments.of(
                        PostgresDataType.JSONARRAY,
                        "{\"[{\"key\": \"value1\"}]\",\"[{\"key\": \"value2\"}]\"}",
                        Arrays.asList("[{\"key\": \"value1\"}]", "[{\"key\": \"value2\"}]")
                ),
                Arguments.of(
                        PostgresDataType.JSONARRAY,
                        "{\"[1, 2, 3]\",\"[4, 5, 6]\"}",
                        Arrays.asList("[1, 2, 3]", "[4, 5, 6]")
                ),
                Arguments.of(PostgresDataType.JSONARRAY, "{}", Collections.emptyList()),

                // JSONB array cases
                Arguments.of(
                        PostgresDataType.JSONBARRAY,
                        "{\"[{\"nested\": {\"array\": [1, 2, 3]}}]\",\"[{\"nested\": {\"array\": [4, 5, 6]}}]\"}",
                        Arrays.asList("[{\"nested\": {\"array\": [1, 2, 3]}}]", "[{\"nested\": {\"array\": [4, 5, 6]}}]")
                ),
                Arguments.of(
                        PostgresDataType.JSONBARRAY,
                        "{\"[true, false]\",\"[null, \"string\"]\"}",
                        Arrays.asList("[true, false]", "[null, \"string\"]")
                ),
                Arguments.of(PostgresDataType.JSONBARRAY, "{}", Collections.emptyList())
        );
    }

    @BeforeEach
    void setUp() {
        handler = new JsonTypeHandler();
    }

    @ParameterizedTest
    @CsvSource({
            "JSON, '{\"key\": \"value\"}'",
            "JSONB, '{\"nested\": {\"array\": [1, 2, 3]}}'"
    })
    public void test_handle_json(PostgresDataType columnType, String value) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }

    @ParameterizedTest
    @MethodSource("provideJsonArrayData")
    public void test_handle_json_array(PostgresDataType columnType, String value, List<String> expected) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);

        assertThat(result, is(instanceOf(List.class)));
        List<String> resultList = (List<String>) result;
        assertEquals(expected.size(), resultList.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), resultList.get(i));
        }
    }

    @Test
    public void test_handle_null_json_array() {
        assertNull(handler.process(PostgresDataType.JSONARRAY, "testColumn", null));
        assertNull(handler.process(PostgresDataType.JSONBARRAY, "testColumn", null));
    }
}
