/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IntegerConverterTests {
    @Test
    void testStringToIntegerConversion() {
        IntegerConverter converter = new IntegerConverter();
        final String stringConstant = "1234";
        assertThat(converter.convert(stringConstant), equalTo(Integer.parseInt(stringConstant)));
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
        final Integer intConstant = (int)1234;
        assertThat(converter.convert(intConstant), equalTo(intConstant));
    }
    @Test
    void testInvalidStringConversion() {
        IntegerConverter converter = new IntegerConverter();
        final Double doubleConstant = 12345678.12345678;
        assertThrows(IllegalArgumentException.class, () -> converter.convert(doubleConstant));
    }
}
