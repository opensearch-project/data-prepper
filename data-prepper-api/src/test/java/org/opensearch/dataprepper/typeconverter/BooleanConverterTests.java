/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Collections;

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
    void testInvalidBooleanConversion() {
        BooleanConverter converter = new BooleanConverter();
        final Map<Object, Object> map = Collections.emptyMap();
        assertThrows(IllegalArgumentException.class, () -> converter.convert(map));
    }
}
