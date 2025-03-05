package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NumericTypeHandlerTest {
    private NumericTypeHandler handler;

    @BeforeEach
    void setUp() {
        handler = new NumericTypeHandler();
    }

    @ParameterizedTest
    @MethodSource("provideNumericTypeData")
    public void test_handle(final PostgresDataType postgresDataType, final String columnName, final Object value, final Object expectedValue) {
        Object result = handler.process(postgresDataType, columnName, value);
        if (result != null) {
            assertThat(result, instanceOf(expectedValue.getClass()));
        }
        assertThat(result, is(expectedValue));
    }

    @Test
    void test_handle_positive_money() {
        PostgresDataType dataType = PostgresDataType.MONEY;
        Object result = handler.process(dataType, "testColumn", "$1200");
        assertInstanceOf(Map.class, result);
        Map<String, Object> moneyMap = (Map<String, Object>) result;
        assertEquals(1200.0, moneyMap.get("amount"));
        assertEquals('$', moneyMap.get("currency"));
    }

    @Test
    void test_handle_negative_money() {
        PostgresDataType dataType = PostgresDataType.MONEY;
        Object result = handler.process(dataType, "testColumn", "-$1200.53");
        assertInstanceOf(Map.class, result);
        Map<String, Object> moneyMap = (Map<String, Object>) result;
        assertEquals(-1200.53, moneyMap.get("amount"));
        assertEquals('$', moneyMap.get("currency"));
    }

    private static Stream<Arguments> provideNumericTypeData() {
        return Stream.of(
                // SMALLINT tests (-32768 to 32767)
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "1", (short)1),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "-32768", (short)-32768),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "32767", (short)32767),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", null, null),

                // SMALLSERIAL tests (1 to 32767)
                Arguments.of(PostgresDataType.SMALLSERIAL, "smallserial_col", "1", (short)1),
                Arguments.of(PostgresDataType.SMALLSERIAL, "smallserial_col", "32767", (short)32767),
                Arguments.of(PostgresDataType.SMALLSERIAL, "smallserial_col", null, null),

                // INTEGER tests (-2,147,483,648 to 2,147,483,647)
                Arguments.of(PostgresDataType.INTEGER, "int_col", "2147483647", 2147483647),
                Arguments.of(PostgresDataType.INTEGER, "int_col", "-2147483648", -2147483648),
                Arguments.of(PostgresDataType.INTEGER, "int_col", "0", 0),
                Arguments.of(PostgresDataType.INTEGER, "int_col", null, null),

                // SERIAL tests ( 1 to 2,147,483,647)
                Arguments.of(PostgresDataType.SERIAL, "serial_col", "2147483647", 2147483647),
                Arguments.of(PostgresDataType.SERIAL, "serial_col", "1", 1),
                Arguments.of(PostgresDataType.SERIAL, "serial_col", null, null),

                // BIGINT tests (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
                Arguments.of(PostgresDataType.BIGINT, "bigint_col", "9223372036854775807", 9223372036854775807L),
                Arguments.of(PostgresDataType.BIGINT, "bigint_col", "-9223372036854775808", -9223372036854775808L),
                Arguments.of(PostgresDataType.BIGINT, "bigint_col", "0", 0L),
                Arguments.of(PostgresDataType.BIGINT, "bigint_col", null, null),

                // BIGSERIAL tests (1 to 9,223,372,036,854,775,807)
                Arguments.of(PostgresDataType.BIGSERIAL, "bigserial_col", "9223372036854775807", 9223372036854775807L),
                Arguments.of(PostgresDataType.BIGSERIAL, "bigserial_col", "1", 1L),
                Arguments.of(PostgresDataType.BIGSERIAL, "bigserial_col", null, null),

                // REAL tests
                Arguments.of(PostgresDataType.REAL, "real_col", Float.toString(123.451234f), 123.451234f),
                Arguments.of(PostgresDataType.REAL, "real_col", Float.toString(-123.45f), -123.45f),
                Arguments.of(PostgresDataType.REAL, "real_col", Float.toString(0.0f), 0.0f),
                Arguments.of(PostgresDataType.REAL, "real_col", Float.toString(Float.MAX_VALUE), Float.MAX_VALUE),
                Arguments.of(PostgresDataType.REAL, "real_col", "-Infinity", Float.NEGATIVE_INFINITY),
                Arguments.of(PostgresDataType.REAL, "real_col", "Infinity", Float.POSITIVE_INFINITY),
                Arguments.of(PostgresDataType.REAL, "real_col", "NaN", Float.NaN),
                Arguments.of(PostgresDataType.REAL, "real_col", null,null),


                // DOUBLE PRECISION tests
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", 123.4567890123412345, 123.4567890123412345),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", -123.45678901234, -123.45678901234),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", 0.0, 0.0),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", Double.MAX_VALUE, Double.MAX_VALUE),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", "-Infinity", Double.NEGATIVE_INFINITY),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", "Infinity", Double.POSITIVE_INFINITY),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", "NaN", Double.NaN),
                Arguments.of(PostgresDataType.DOUBLE_PRECISION, "double_precision_col", null, null),

                // NUMERIC tests
                Arguments.of(PostgresDataType.NUMERIC, "numeric_col", "123.45", "123.45"),
                Arguments.of(PostgresDataType.NUMERIC, "numeric_col", "-123.45", "-123.45"),
                Arguments.of(PostgresDataType.NUMERIC, "numeric_col", "0", "0"),
                Arguments.of(PostgresDataType.NUMERIC, "numeric_col", null, null)

        );
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.TEXT, "invalid_col", "not_a_number");
        });
    }

    @Test
    public void test_handleInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "int_col", "not_a_number");
        });
    }
}
