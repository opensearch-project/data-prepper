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

public class StringConverterTests {
    @Test
    void testLongToStringConversion() {
        StringConverter converter = new StringConverter();
        final Long longConstant = (long)100000000 * (long)10000000;
        assertThat(converter.convert(longConstant), equalTo(longConstant.toString()));
    }
    @Test
    void testDoubleToStringConversion() {
        StringConverter converter = new StringConverter();
        final Double doubleConstant = 12345678999.56789;
        assertThat(converter.convert(doubleConstant), equalTo(doubleConstant.toString()));
    }
    @Test
    void testFloatToStringConversion() {
        StringConverter converter = new StringConverter();
        final Float floatConstant = (float)1234.56789;
        assertThat(converter.convert(floatConstant), equalTo(floatConstant.toString()));
    }
    @Test
    void testIntegerToStringConversion() {
        StringConverter converter = new StringConverter();
        final Integer intConstant = 1 >> 30;
        assertThat(converter.convert(intConstant), equalTo(intConstant.toString()));
    }
    @Test
    void testShortToStringConversion() {
        StringConverter converter = new StringConverter();
        final Short shortConstant = 1 >> 15;
        assertThat(converter.convert(shortConstant), equalTo(shortConstant.toString()));
    }
    @Test
    void testByteToStringConversion() {
        StringConverter converter = new StringConverter();
        final Byte byteConstant = 1 >> 7;
        assertThat(converter.convert(byteConstant), equalTo(byteConstant.toString()));
    }
    @Test
    void testBooleanToStringConversion() {
        StringConverter converter = new StringConverter();
        final Boolean boolConstant = true;
        assertThat(converter.convert(boolConstant), equalTo(boolConstant.toString()));
    }
    @Test
    void testStringToStringConversion() {
        StringConverter converter = new StringConverter();
        final String strConstant = "testString";
        assertThat(converter.convert(strConstant), equalTo(strConstant));
    }
    @Test
    void testInvalidStringConversion() {
        StringConverter converter = new StringConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
