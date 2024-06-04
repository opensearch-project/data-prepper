/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.stream.Stream;

public class IntegerConverterTests {
    @Test
    void testStringToIntegerConversion() {
        IntegerConverter converter = new IntegerConverter();
        final String stringConstant = "1234";
        assertThat(converter.convert(stringConstant), equalTo(Integer.parseInt(stringConstant)));
        assertThat(converter.convert(stringConstant, () -> 0), equalTo(Integer.parseInt(stringConstant)));
    }
    @Test
    void testFloatToIntegerConversion() {
        IntegerConverter converter = new IntegerConverter();
        final Float floatConstant = (float)1234.56789;
        assertThat(converter.convert(floatConstant), equalTo((int)(float)floatConstant));
    }
    @Test
    void testBooleanToIntegerConversion() {
        IntegerConverter converter = new IntegerConverter();
        final Boolean boolFalseConstant = false;
        assertThat(converter.convert(boolFalseConstant), equalTo(0));
        final Boolean boolTrueConstant = true;
        assertThat(converter.convert(boolTrueConstant), equalTo(1));
    }
    @Test
    void testIntegerToIntegerConversion() {
        IntegerConverter converter = new IntegerConverter();
        final Integer intConstant = 1234;
        assertThat(converter.convert(intConstant), equalTo(intConstant));
    }
    @ParameterizedTest
    @MethodSource("BigDecimalValueProvider")
    void testBigDecimalToIntegerConversion(BigDecimal bigDecimalConstant, int expectedIntegerValue) {
        IntegerConverter converter = new IntegerConverter();
        assertThat(converter.convert(bigDecimalConstant), equalTo(expectedIntegerValue));
    }
    private static Stream<Arguments> BigDecimalValueProvider() {
        return Stream.of(
            Arguments.of(new BigDecimal ("0"), 0),
            Arguments.of(new BigDecimal ("0.0"), 0),
            Arguments.of(new BigDecimal ("0.00000000000000000000000"), 0),
            Arguments.of(BigDecimal.ZERO, BigDecimal.ZERO.intValue()),
            Arguments.of(new BigDecimal ("1"), 1),
            Arguments.of(new BigDecimal ("1703908514.045833"), 1703908514),
            Arguments.of(new BigDecimal ("1.00000000000000000000000"), 1),
            Arguments.of(new BigDecimal ("-12345678.12345"), -12345678),
            Arguments.of(BigDecimal.ONE, BigDecimal.ONE.intValue()),
            Arguments.of(new BigDecimal("1.7976931348623157E+308"), 0),
            Arguments.of(new BigDecimal(Integer.MAX_VALUE), Integer.MAX_VALUE),
            Arguments.of(new BigDecimal(Integer.MIN_VALUE), Integer.MIN_VALUE)
        );
    }
    @Test
    void testInvalidStringConversion() {
        IntegerConverter converter = new IntegerConverter();
        final Double doubleConstant = 12345678.12345678;
        assertThrows(IllegalArgumentException.class, () -> converter.convert(doubleConstant));
    }
}
