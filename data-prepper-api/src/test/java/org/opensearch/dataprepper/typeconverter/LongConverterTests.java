/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
 
 public class LongConverterTests {
     @ParameterizedTest
     @ValueSource(strings = {"1234", "5678"})
     void testStringToLongConversion(String stringValue) {
         LongConverter converter = new LongConverter();
         assertThat(converter.convert(stringValue), equalTo(Long.parseLong(stringValue)));
         assertThat(converter.convert(stringValue, () -> 0), equalTo(Long.parseLong(stringValue)));
     }
     @ParameterizedTest
     @ValueSource(floats = {(float)1234.56789, Float.MAX_VALUE, Float.MIN_VALUE})
     void testfloatToLongConversion(float floatValue) {
         LongConverter converter = new LongConverter();
         assertThat(converter.convert(floatValue), equalTo((long) floatValue));
     }
     @ParameterizedTest
     @ValueSource(doubles = {12345678.12345678, 2.0 * Integer.MAX_VALUE, Double.MAX_VALUE, Double.MIN_VALUE})
     void testDoubleToLongConversion(double doubleValue) {
        LongConverter converter = new LongConverter();
        assertThat(converter.convert(doubleValue), equalTo((long) doubleValue));
     }
     @ParameterizedTest
     @ValueSource(booleans = {false,true})
     void testBooleanToLongConversion(boolean booleanValue) {
         LongConverter converter = new LongConverter();
         if (booleanValue)
            assertThat(converter.convert(booleanValue), equalTo(1L));
         else
            assertThat(converter.convert(booleanValue), equalTo(0L));
     }
     @ParameterizedTest
     @ValueSource(ints = {1234, Integer.MAX_VALUE, Integer.MIN_VALUE})
     void testIntToLongConverstion(int intValue){
        LongConverter converter = new LongConverter();
        assertThat(converter.convert(intValue), equalTo((long)intValue));
     }
     @ParameterizedTest
     @ValueSource(longs = {(long)1234, Long.MAX_VALUE, Long.MIN_VALUE})
     void testLongToLongConversion(long longValue) {
         LongConverter converter = new LongConverter();
         assertThat(converter.convert(longValue), equalTo(longValue));
     }
    
    @ParameterizedTest
    @MethodSource("BigDecimalValueProvider")
    void testBigDecimalToDoubleConversion(BigDecimal bigDecimalConstant, long expectedlongValue) {
        LongConverter converter = new LongConverter();
        assertThat(converter.convert(bigDecimalConstant), equalTo(expectedlongValue));
    }
    private static Stream<Arguments> BigDecimalValueProvider() {
        return Stream.of(
            Arguments.of(new BigDecimal ("0.00000000000000000000000"), (long)0),
            Arguments.of(BigDecimal.ZERO, BigDecimal.ZERO.longValue()),
            Arguments.of(new BigDecimal ("1"), (long)1),
            Arguments.of(new BigDecimal ("1703908514.045833"), (long)1703908514),
            Arguments.of(new BigDecimal ("1.00000000000000000000000"), (long)1),
            Arguments.of(new BigDecimal ("-1234567891.12345"), (long)-1234567891),
            Arguments.of(BigDecimal.ONE, BigDecimal.ONE.longValue()),
            Arguments.of(new BigDecimal("1.7976931348623157E+308"), (long)0),
            Arguments.of(new BigDecimal(Integer.MAX_VALUE), (long)Integer.MAX_VALUE),
            Arguments.of(new BigDecimal(Integer.MIN_VALUE), (long)Integer.MIN_VALUE),   
            Arguments.of(new BigDecimal(Long.MAX_VALUE), Long.MAX_VALUE),
            Arguments.of(new BigDecimal(Long.MIN_VALUE), Long.MIN_VALUE),
            Arguments.of(new BigDecimal("267694723"), (long)267694723)

        );
    }
     @Test
     void testInvalidStringConversion() {
         LongConverter converter = new LongConverter();
         assertThrows(IllegalArgumentException.class, () -> converter.convert(new Object()));
     }
 }
 