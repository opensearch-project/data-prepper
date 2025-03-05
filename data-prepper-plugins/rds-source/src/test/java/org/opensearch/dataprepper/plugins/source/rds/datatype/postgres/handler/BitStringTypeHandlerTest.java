package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;

import java.math.BigInteger;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BitStringTypeHandlerTest {
    private BitStringTypeHandler handler;

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

    @Test
    public void test_handleInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> {
            handler.process(PostgresDataType.INTEGER, "invalid_col", 123);
        });
    }

    private static Stream<Arguments> provideBitTypeData() {
        return Stream.of(
                Arguments.of(PostgresDataType.BIT, "10101", BigInteger.valueOf(21)),
                Arguments.of(PostgresDataType.VARBIT, "1010111",  BigInteger.valueOf(87))
        );
    }
}
