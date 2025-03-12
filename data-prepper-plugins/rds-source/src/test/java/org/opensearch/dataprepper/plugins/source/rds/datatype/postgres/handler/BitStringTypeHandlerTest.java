package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.math.BigInteger;
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

public class BitStringTypeHandlerTest {
    private BitStringTypeHandler handler;

    private static Stream<Arguments> provideBitTypeData() {
        return Stream.of(
                Arguments.of(PostgresDataType.BIT, "10101", BigInteger.valueOf(21)),
                Arguments.of(PostgresDataType.VARBIT, "1010111", BigInteger.valueOf(87))
        );
    }

    private static Stream<Arguments> provideBitAndVarBitArrayData() {
        return Stream.of(
                // BITARRAY cases
                Arguments.of(
                        PostgresDataType.BITARRAY,
                        "bit_array_col",
                        "{1010,1100,1111}",
                        Arrays.asList(new BigInteger("1010", 2), new BigInteger("1100", 2), new BigInteger("1111", 2))
                ),
                Arguments.of(
                        PostgresDataType.BITARRAY,
                        "bit_array_col",
                        "{}",
                        Collections.emptyList()
                ),
                Arguments.of(
                        PostgresDataType.BITARRAY,
                        "bit_array_col",
                        null,
                        null
                ),

                // VARBITARRAY cases
                Arguments.of(
                        PostgresDataType.VARBITARRAY,
                        "varbit_array_col",
                        "{101010,11001100,111100001111}",
                        Arrays.asList(new BigInteger("101010", 2), new BigInteger("11001100", 2), new BigInteger("111100001111", 2))
                ),
                Arguments.of(
                        PostgresDataType.VARBITARRAY,
                        "varbit_array_col",
                        "{}",
                        Collections.emptyList()
                ),
                Arguments.of(
                        PostgresDataType.VARBITARRAY,
                        "varbit_array_col",
                        null,
                        null
                )
        );
    }

    @BeforeEach
    void setUp() {
        handler = new BitStringTypeHandler();
    }

    @ParameterizedTest
    @MethodSource("provideBitTypeData")
    public void test_handle_bit_string(PostgresDataType columnType, String value, BigInteger expected) {
        String columnName = "testColumn";
        Object result = handler.process(columnType, columnName, value);
        assertThat(result, is(instanceOf(BigInteger.class)));
        assertThat(result, is(expected));
    }

    @ParameterizedTest
    @MethodSource("provideBitAndVarBitArrayData")
    public void test_handle_bit_and_varbit_array(final PostgresDataType postgresDataType, final String columnName, final String value, final List<BigInteger> expectedValue) {
        Object result = handler.process(postgresDataType, columnName, value);

        if (result != null) {
            assertThat(result, instanceOf(List.class));
            List<BigInteger> resultList = (List<BigInteger>) result;
            assertEquals(expectedValue.size(), resultList.size());
            for (int i = 0; i < expectedValue.size(); i++) {
                assertEquals(expectedValue.get(i), resultList.get(i));
            }
        } else {
            assertNull(expectedValue);
        }
    }

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }
}
