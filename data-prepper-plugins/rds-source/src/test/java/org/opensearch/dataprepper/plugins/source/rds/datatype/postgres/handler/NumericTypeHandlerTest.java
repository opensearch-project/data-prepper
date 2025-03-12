package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class NumericTypeHandlerTest {
    private NumericTypeHandler handler;

    private static Stream<Arguments> provideNumericTypeData() {
        return Stream.of(
                // SMALLINT tests (-32768 to 32767)
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "1", (short) 1),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "-32768", (short) -32768),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", "32767", (short) 32767),
                Arguments.of(PostgresDataType.SMALLINT, "smallint_col", null, null),

                // SMALLSERIAL tests (1 to 32767)
                Arguments.of(PostgresDataType.SMALLSERIAL, "smallserial_col", "1", (short) 1),
                Arguments.of(PostgresDataType.SMALLSERIAL, "smallserial_col", "32767", (short) 32767),
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
                Arguments.of(PostgresDataType.REAL, "real_col", null, null),


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

    private static Stream<Arguments> provideNumericArrayTypeData() {
        return Stream.of(
                // SMALLINT array tests
                Arguments.of(PostgresDataType.INT2ARRAY, "smallint_array_col", "{1,2,3}", Arrays.asList((short) 1, (short) 2, (short) 3)),
                Arguments.of(PostgresDataType.INT2ARRAY, "smallint_array_col", "{-32768,0,32767}", Arrays.asList((short) -32768, (short) 0, (short) 32767)),
                Arguments.of(PostgresDataType.INT2ARRAY, "smallint_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.INT2ARRAY, "smallint_array_col", null, null),

                // INTEGER array tests
                Arguments.of(PostgresDataType.INT4ARRAY, "int_array_col", "{2147483647,-2147483648,0}", Arrays.asList(2147483647, -2147483648, 0)),
                Arguments.of(PostgresDataType.INT4ARRAY, "int_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.INT4ARRAY, "int_array_col", null, null),

                // BIGINT array tests
                Arguments.of(PostgresDataType.INT8ARRAY, "bigint_array_col", "{9223372036854775807,-9223372036854775808,0}", Arrays.asList(9223372036854775807L, -9223372036854775808L, 0L)),
                Arguments.of(PostgresDataType.INT8ARRAY, "bigint_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.INT8ARRAY, "bigint_array_col", null, null),

                // REAL array tests
                Arguments.of(PostgresDataType.FLOAT4ARRAY, "real_array_col", "{123.45,-123.45,0.0,Infinity,-Infinity,NaN}", Arrays.asList(123.45f, -123.45f, 0.0f, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN)),
                Arguments.of(PostgresDataType.FLOAT4ARRAY, "real_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.FLOAT4ARRAY, "real_array_col", null, null),

                // DOUBLE PRECISION array tests
                Arguments.of(PostgresDataType.FLOAT8ARRAY, "double_precision_array_col", "{123.4567890123412345,-123.45678901234,0.0,Infinity,-Infinity,NaN}", Arrays.asList(123.4567890123412345, -123.45678901234, 0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN)),
                Arguments.of(PostgresDataType.FLOAT8ARRAY, "double_precision_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.FLOAT8ARRAY, "double_precision_array_col", null, null),

                // NUMERIC array tests
                Arguments.of(PostgresDataType.NUMERICARRAY, "numeric_array_col", "{123.45,-123.45,0}", Arrays.asList("123.45", "-123.45", "0")),
                Arguments.of(PostgresDataType.NUMERICARRAY, "numeric_array_col", "{}", Collections.emptyList()),
                Arguments.of(PostgresDataType.NUMERICARRAY, "numeric_array_col", null, null)
        );
    }

    private static Stream<Arguments> provideMoneyArrayTypeData() {
        return Stream.of(
                // Normal money array case
                Arguments.of(
                        PostgresDataType.MONEYARRAY,
                        "money_array_col",
                        "{\"$1,234.56\",\"-$789.01\",\"$0.00\"}",
                        Arrays.asList(
                                Map.of("amount", 1234.56, "currency", '$'),
                                Map.of("amount", -789.01, "currency", '$'),
                                Map.of("amount", 0.00, "currency", '$')
                        )
                ),
                // Empty money array case
                Arguments.of(
                        PostgresDataType.MONEYARRAY,
                        "money_array_col",
                        "{}",
                        Collections.emptyList()
                ),
                // Null money array case
                Arguments.of(
                        PostgresDataType.MONEYARRAY,
                        "money_array_col",
                        null,
                        null
                )
        );
    }

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

    @ParameterizedTest
    @MethodSource("provideNumericArrayTypeData")
    public void test_handle_array(final PostgresDataType postgresDataType, final String columnName, final Object value, final Object expectedValue) {
        Object result = handler.process(postgresDataType, columnName, value);
        if (result != null) {
            assertThat(result, instanceOf(List.class));
            assertEquals(expectedValue, result);
        } else {
            assertNull(expectedValue);
        }
    }

    @ParameterizedTest
    @MethodSource("provideMoneyArrayTypeData")
    public void test_handle_money_array(final PostgresDataType postgresDataType, final String columnName, final String value, final List<Map<String, Object>> expectedValue) {
        Object result = handler.process(postgresDataType, columnName, value);

        if (result != null) {
            assertThat(result, instanceOf(List.class));

            List<Map<String, Object>> resultList = (List<Map<String, Object>>) result;
            assertEquals(expectedValue.size(), resultList.size());

            for (int i = 0; i < expectedValue.size(); i++) {
                Map<String, Object> expectedMap = expectedValue.get(i);
                Map<String, Object> resultMap = resultList.get(i);

                assertEquals(expectedMap.get("currency"), resultMap.get("currency"));
                assertEquals((Double) expectedMap.get("amount"), (Double) resultMap.get("amount"));
            }
        } else {
            assertNull(expectedValue);
        }
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
