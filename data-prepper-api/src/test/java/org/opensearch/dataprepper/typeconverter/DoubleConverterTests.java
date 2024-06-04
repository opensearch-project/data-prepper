/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DoubleConverterTests {
    @Test
    void testStringToDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant), equalTo(Double.parseDouble(stringConstant)));
        assertThat(converter.convert(stringConstant, () -> 0), equalTo(Double.parseDouble(stringConstant)));
    }
    @Test
    void testIntegerToDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final Integer intConstant = 12345;
        assertThat(converter.convert(intConstant), equalTo((double)intConstant));
    }
    @Test
    void testBooleanToDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final Boolean boolFalseConstant = false;
        assertThat(converter.convert(boolFalseConstant), equalTo(0.0));
        final Boolean boolTrueConstant = true;
        assertThat(converter.convert(boolTrueConstant), equalTo(1.0));
    }
    @Test
    void testDoubleToDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final Double doubleConstant = 12345.123;
        assertThat(converter.convert(doubleConstant), equalTo(doubleConstant));
    }
    @ParameterizedTest
    @MethodSource("BigDecimalValueProvider")
    void testBigDecimalToDoubleConversion(BigDecimal bigDecimalConstant, double expectedDoubleValue) {
        DoubleConverter converter = new DoubleConverter();
        assertThat(converter.convert(bigDecimalConstant), equalTo(expectedDoubleValue));
    }
    private static Stream<Arguments> BigDecimalValueProvider() {
        return Stream.of(
            Arguments.of(new BigDecimal ("0"), (double)0),
            Arguments.of(new BigDecimal ("0.0"), (double)0),
            Arguments.of(new BigDecimal ("0.00000000000000000000000"), 0.00000000000000000000000),
            Arguments.of(BigDecimal.ZERO, BigDecimal.ZERO.doubleValue()),
            Arguments.of(new BigDecimal ("1"), (double)1),
            Arguments.of(new BigDecimal ("1703908514.045833"), 1703908514.045833),
            Arguments.of(new BigDecimal ("1.00000000000000000000000"), 1.00000000000000000000000),
            Arguments.of(new BigDecimal ("-12345678912.12345"), -12345678912.12345),
            Arguments.of(BigDecimal.ONE, BigDecimal.ONE.doubleValue()),
            Arguments.of(new BigDecimal("1.7976931348623157E+308"), 1.7976931348623157E+308),
            Arguments.of(new BigDecimal(Double.MAX_VALUE), Double.MAX_VALUE),
            Arguments.of(new BigDecimal(Double.MIN_VALUE), Double.MIN_VALUE)
        );
    }
    @Test
    void testInvalidDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
