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
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BigDecimalConverterTests {
    @Test
    void testStringToBigDecimalConversion() {
        BigDecimalConverter converter = new BigDecimalConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant), equalTo(new BigDecimal(stringConstant)));
    }
    @Test
    void testIntegerToBigDecimalConversion() {
        BigDecimalConverter converter = new BigDecimalConverter();
        final int intConstant = 12345;
        assertThat(converter.convert(intConstant), equalTo(BigDecimal.valueOf(intConstant)));
    }
    @Test
    void testBooleanToBigDecimalConversion() {
        BigDecimalConverter converter = new BigDecimalConverter();
        final Boolean boolFalseConstant = false;
        assertThat(converter.convert(boolFalseConstant), equalTo(BigDecimal.valueOf(0)));
        final Boolean boolTrueConstant = true;
        assertThat(converter.convert(boolTrueConstant), equalTo(BigDecimal.valueOf(1)));
    }

    @Test
    void testBigDecimalToBigDecimalConversion() {
        BigDecimalConverter converter = new BigDecimalConverter();
        BigDecimal bigDecimal = new BigDecimal("12345.6789");
        assertThat(converter.convert(bigDecimal), equalTo(bigDecimal));
    }

    @ParameterizedTest
    @MethodSource("decimalToBigDecimalValueProvider")
    void testDoubleToBigDecimalConversion(BigDecimal expectedBigDecimal, double actualValue, int scale) {
        BigDecimalConverter converter = new BigDecimalConverter();
        if(scale!=0) {
            converter.setScale(scale);
            expectedBigDecimal = expectedBigDecimal.setScale(scale, RoundingMode.HALF_EVEN);
        }
        assertThat(converter.convert(actualValue), equalTo(expectedBigDecimal));
    }

    private static Stream<Arguments> decimalToBigDecimalValueProvider() {
        return Stream.of(
            Arguments.of(new BigDecimal ("0.0"), 0, 1),
            Arguments.of(new BigDecimal ("0.0"), 0.0, 1),
            Arguments.of(new BigDecimal ("0.00000000000000000000000"), 0.00000000000000000000000, 1),
            Arguments.of(BigDecimal.ZERO, BigDecimal.ZERO.doubleValue(), 1),
            Arguments.of(new BigDecimal ("1"), (double)1, 1),
            Arguments.of(new BigDecimal ("1703908514.045833"), 1703908514.045833, 6),
            Arguments.of(new BigDecimal ("1.00000000000000000000000"), 1.00000000000000000000000, 1),
            Arguments.of(new BigDecimal ("-12345678912.12345"), -12345678912.12345, 1),
            Arguments.of(BigDecimal.ONE, BigDecimal.ONE.doubleValue(), 1),
            Arguments.of(new BigDecimal("1.7976931348623157E+308"), 1.7976931348623157E+308, 0),
            Arguments.of(BigDecimal.valueOf(Double.MAX_VALUE), Double.MAX_VALUE, 0),
            Arguments.of(BigDecimal.valueOf(Double.MIN_VALUE), Double.MIN_VALUE, 0)
        );
    }
    @Test
    void testInvalidBigDecimalConversion() {
        BigDecimalConverter converter = new BigDecimalConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
