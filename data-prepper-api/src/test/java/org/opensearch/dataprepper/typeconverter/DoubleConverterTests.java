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

public class DoubleConverterTests {
    @Test
    void testStringToDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final String stringConstant = "12345678912.12345";
        assertThat(converter.convert(stringConstant), equalTo(Double.parseDouble(stringConstant)));
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
        final Double doubleConstant = (double)12345.123;
        assertThat(converter.convert(doubleConstant), equalTo(doubleConstant));
    }
    @Test
    void testInvalidDoubleConversion() {
        DoubleConverter converter = new DoubleConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
