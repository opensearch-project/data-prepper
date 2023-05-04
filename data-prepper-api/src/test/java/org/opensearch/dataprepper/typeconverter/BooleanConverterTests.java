/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BooleanConverterTests {
    @Test
    void testStringToBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant), equalTo(Boolean.parseBoolean(stringConstant)));
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
    @Test
    void testInvalidBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
