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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class StringTypeHandlerTest {
    private StringTypeHandler handler;

    private static Stream<Arguments> provideCharArrayData() {
        return Stream.of(
                // TEXTARRAY cases
                Arguments.of(PostgresDataType.TEXTARRAY, "{Hello,World}", Arrays.asList("Hello", "World")),
                Arguments.of(PostgresDataType.TEXTARRAY, "{OpenSearch,DataPrepper}", Arrays.asList("OpenSearch", "DataPrepper")),
                Arguments.of(PostgresDataType.TEXTARRAY, "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.TEXTARRAY, null, null),

                // VARCHARARRAY cases
                Arguments.of(PostgresDataType.VARCHARARRAY, "{Hello,World}", Arrays.asList("Hello", "World")),
                Arguments.of(PostgresDataType.VARCHARARRAY, "{OpenSearch,DataPrepper}", Arrays.asList("OpenSearch", "DataPrepper")),
                Arguments.of(PostgresDataType.VARCHARARRAY,
                        "{normal,\"\\\"double quoted\\\"\",\"'single quoted'\",\"\\\\backslashed\\\\\",\"\\\"quoted with \\\\\\\"nested\\\\\\\" quotes\\\"\"}",
                        Arrays.asList("normal", "\"double quoted\"","'single quoted'","\\backslashed\\","\"quoted " +
                                "with \\\"nested\\\" quotes\"")),
                Arguments.of(PostgresDataType.VARCHARARRAY, "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.VARCHARARRAY, null, null),

                // BPCHARARRAY cases
                Arguments.of(PostgresDataType.BPCHARARRAY, "{Hello ,World }", Arrays.asList("Hello", "World")), // Note the padding spaces
                Arguments.of(PostgresDataType.BPCHARARRAY, "{OpenSearch ,DataPrepper }", Arrays.asList("OpenSearch", "DataPrepper")),
                Arguments.of(PostgresDataType.BPCHARARRAY, "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.BPCHARARRAY, null, null)
        );
    }

    @BeforeEach
    void setUp() {
        handler = new StringTypeHandler();
    }

    @ParameterizedTest
    @CsvSource({
            "TEXT, Hello, World!",
            "VARCHAR, Hello, World!",
            "BPCHAR, Hello, World!"
    })
    public void test_handle_text_string(PostgresDataType columnType, String value) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);
        assertThat(result, is(instanceOf(String.class)));
        assertThat(result, is(value));
    }

    @ParameterizedTest
    @MethodSource("provideCharArrayData")
    public void test_handle_char_array(PostgresDataType columnType, String value, List<String> expected) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);

        if (result != null) {
            assertThat(result, is(instanceOf(List.class)));

            List<String> resultList = (List<String>) result;
            assertEquals(expected.size(), resultList.size());

            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i), resultList.get(i));
            }
        } else {
            assertNull(expected);
        }
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}
