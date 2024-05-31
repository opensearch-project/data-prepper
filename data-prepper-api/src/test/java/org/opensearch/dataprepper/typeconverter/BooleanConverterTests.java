/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.stream.Stream;

public class BooleanConverterTests {

    @Test
    void testStringToBooleanConversionWithArguments() {
        BooleanConverter converter = new BooleanConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant, () -> 0), equalTo(false));
    }

    @Test
    void testStringToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant), equalTo(false));
    }
    @Test
    void testIntegerToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Integer intTrueConstant = 12345;
        assertThat(converter.convert(intTrueConstant), equalTo(true));
        final Integer intFalseConstant = 0;
        assertThat(converter.convert(intFalseConstant), equalTo(false));
    }
    @Test
    void testDoubleToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Double doubleTrueConstant = 12345.0;
        assertThat(converter.convert(doubleTrueConstant), equalTo(true));
        final Double doubleFalseConstant = 0.0;
        assertThat(converter.convert(doubleFalseConstant), equalTo(false));
    }
    @Test
    void testLongToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Long longTrueConstant = (long)1234578912;
        assertThat(converter.convert(longTrueConstant), equalTo(true));
        final Long longFalseConstant = (long)0;
        assertThat(converter.convert(longFalseConstant), equalTo(false));
    }
    @Test
    void testShortToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Short shortTrueConstant = 12345;
        assertThat(converter.convert(shortTrueConstant), equalTo(true));
        final Short shortFalseConstant = 0;
        assertThat(converter.convert(shortFalseConstant), equalTo(false));
    }
    @Test
    void testByteToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Byte byteTrueConstant = 12;
        assertThat(converter.convert(byteTrueConstant), equalTo(true));
        final Byte byteFalseConstant = 0;
        assertThat(converter.convert(byteFalseConstant), equalTo(false));
    }
    @Test
    void testFloatToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Float floatTrueConstant = (float)12345.0;
        assertThat(converter.convert(floatTrueConstant), equalTo(true));
        final Float floatFalseConstant = (float)0.0;
        assertThat(converter.convert(floatFalseConstant), equalTo(false));
    }
    @Test
    void testBooleanToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Boolean boolTrueConstant = true;
        assertThat(converter.convert(boolTrueConstant), equalTo(true));
        final Boolean boolFalseConstant = false;
        assertThat(converter.convert(boolFalseConstant), equalTo(false));
    }
    @ParameterizedTest
    @MethodSource("BigDecimalValueProvider")
    void testBigDecimalToBooleanConversion(BigDecimal input, boolean expected_boolean) {
        BooleanConverter converter = new BooleanConverter();
        assertThat(converter.convert(input), equalTo(expected_boolean));
    }
    private static Stream<Arguments> BigDecimalValueProvider() {
        return Stream.of(
            Arguments.of(new BigDecimal ("0"), false),
            Arguments.of(new BigDecimal ("0.0"), false),
            Arguments.of(new BigDecimal ("0.00000000000000000000000"), false),
            Arguments.of(BigDecimal.ZERO, false),
            Arguments.of(new BigDecimal ("1"), true),
            Arguments.of(new BigDecimal ("1703908514.045833"), true),
            Arguments.of(new BigDecimal ("1.00000000000000000000000"), true),
            Arguments.of(new BigDecimal ("-12345678912.12345"), true),
            Arguments.of(BigDecimal.ONE, true),
            Arguments.of(BigDecimal.valueOf(Double.MAX_VALUE), true),
            Arguments.of(BigDecimal.valueOf(Double.MIN_VALUE), true)
        );
    }

    @Test
    void testInvalidBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
