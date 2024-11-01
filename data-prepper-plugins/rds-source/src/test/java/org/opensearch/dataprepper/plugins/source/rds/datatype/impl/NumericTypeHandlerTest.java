package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class NumericTypeHandlerTest {
    @ParameterizedTest
    @MethodSource("provideNumericTypeData")
    public void test_handle(final MySQLDataType mySQLDataType, final String columnName, final Object value, final Object expectedValue) {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), List.of(columnName), List.of(columnName),
                Collections.emptyMap(), Collections.emptyMap());
        final DataTypeHandler numericTypeHandler = new NumericTypeHandler();
        String result = numericTypeHandler.handle(mySQLDataType, columnName, value, metadata);

        assertThat(result, is(expectedValue));
    }

    private static Stream<Arguments> provideNumericTypeData() {
        return Stream.of(
                // TINYINT tests (signed: -128 to 127)
                Arguments.of(MySQLDataType.TINYINT, "tinyint_col", (byte)1, "1"),
                Arguments.of(MySQLDataType.TINYINT, "tinyint_col", (byte)-128, "-128"),
                Arguments.of(MySQLDataType.TINYINT, "tinyint_col", (byte)127, "127"),
                Arguments.of(MySQLDataType.TINYINT, "tinyint_col", null, null),

                // TINYINT UNSIGNED tests (0 to 255)
                Arguments.of(MySQLDataType.TINYINT_UNSIGNED, "tinyint_unsigned_col", (short)0, "0"),
                Arguments.of(MySQLDataType.TINYINT_UNSIGNED, "tinyint_unsigned_col", (short)255, "255"),
                Arguments.of(MySQLDataType.TINYINT_UNSIGNED, "tinyint_unsigned_col", (short)128, "128"),

                // SMALLINT tests (signed: -32,768 to 32,767)
                Arguments.of(MySQLDataType.SMALLINT, "smallint_col", (short)32767, "32767"),
                Arguments.of(MySQLDataType.SMALLINT, "smallint_col", (short)-32768, "-32768"),
                Arguments.of(MySQLDataType.SMALLINT, "smallint_col", (short)0, "0"),

                // SMALLINT UNSIGNED tests (0 to 65,535)
                Arguments.of(MySQLDataType.SMALLINT_UNSIGNED, "smallint_unsigned_col", 0, "0"),
                Arguments.of(MySQLDataType.SMALLINT_UNSIGNED, "smallint_unsigned_col", 65535, "65535"),
                Arguments.of(MySQLDataType.SMALLINT_UNSIGNED, "smallint_unsigned_col", 32768, "32768"),

                // INTEGER/INT tests (signed: -2,147,483,648 to 2,147,483,647)
                Arguments.of(MySQLDataType.INT, "int_col", 2147483647, "2147483647"),
                Arguments.of(MySQLDataType.INT, "int_col", -2147483648, "-2147483648"),
                Arguments.of(MySQLDataType.INT, "int_col", 0, "0"),

                // INTEGER/INT UNSIGNED tests (0 to 4,294,967,295)
                Arguments.of(MySQLDataType.INT_UNSIGNED, "int_unsigned_col", 4294967295L, "4294967295"),
                Arguments.of(MySQLDataType.INT_UNSIGNED, "int_unsigned_col", 0L, "0"),
                Arguments.of(MySQLDataType.INT_UNSIGNED, "int_unsigned_col", 2147483648L, "2147483648"),

                // BIGINT tests (signed: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
                Arguments.of(MySQLDataType.BIGINT, "bigint_col", 9223372036854775807L, "9223372036854775807"),
                Arguments.of(MySQLDataType.BIGINT, "bigint_col", -9223372036854775808L, "-9223372036854775808"),
                Arguments.of(MySQLDataType.BIGINT, "bigint_col", 0L, "0"),

                // BIGINT UNSIGNED tests (0 to 18,446,744,073,709,551,615)
                Arguments.of(MySQLDataType.BIGINT_UNSIGNED, "bigint_unsigned_col", new BigInteger("18446744073709551615"), "18446744073709551615"),
                Arguments.of(MySQLDataType.BIGINT_UNSIGNED, "bigint_unsigned_col", BigInteger.ZERO, "0"),
                Arguments.of(MySQLDataType.BIGINT_UNSIGNED, "bigint_unsigned_col", new BigInteger("9223372036854775808"), "9223372036854775808"),

                // DECIMAL/NUMERIC tests
                Arguments.of(MySQLDataType.DECIMAL, "decimal_col", new BigDecimal("123.45"), "123.45"),
                Arguments.of(MySQLDataType.DECIMAL, "decimal_col", new BigDecimal("-123.45"), "-123.45"),
                Arguments.of(MySQLDataType.DECIMAL, "decimal_col", new BigDecimal("0.0"), "0.0"),
                Arguments.of(MySQLDataType.DECIMAL, "decimal_col", new BigDecimal("999999.99"), "999999.99"),

                // FLOAT tests
                Arguments.of(MySQLDataType.FLOAT, "float_col", 123.45f, "123.45"),
                Arguments.of(MySQLDataType.FLOAT, "float_col", -123.45f, "-123.45"),
                Arguments.of(MySQLDataType.FLOAT, "float_col", 0.0f, "0.0"),
                Arguments.of(MySQLDataType.FLOAT, "float_col", Float.MAX_VALUE, String.valueOf(Float.MAX_VALUE)),

                // DOUBLE tests
                Arguments.of(MySQLDataType.DOUBLE, "double_col", 123.45678901234, "123.45678901234"),
                Arguments.of(MySQLDataType.DOUBLE, "double_col", -123.45678901234, "-123.45678901234"),
                Arguments.of(MySQLDataType.DOUBLE, "double_col", 0.0, "0.0"),
                Arguments.of(MySQLDataType.DOUBLE, "double_col", Double.MAX_VALUE, String.valueOf(Double.MAX_VALUE))
        );
    }

    @Test
    public void test_handleInvalidType() {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                List.of("invalid_col"), List.of("invalid_col"),
                Collections.emptyMap(), Collections.emptyMap());
        final DataTypeHandler numericTypeHandler = new NumericTypeHandler();

        assertThrows(IllegalArgumentException.class, () -> {
            numericTypeHandler.handle(MySQLDataType.INT_UNSIGNED, "invalid_col", "not_a_number", metadata);
        });
    }

    @Test
    public void test_handleInvalidValue() {
        final TableMetadata metadata = new TableMetadata(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                List.of("int_col"), List.of("int_col"),
                Collections.emptyMap(), Collections.emptyMap());
        final DataTypeHandler numericTypeHandler = new NumericTypeHandler();

        assertThrows(IllegalArgumentException.class, () -> {
            numericTypeHandler.handle(MySQLDataType.INT, "int_col", "not_a_number", metadata);
        });
    }
}
