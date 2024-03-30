/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.dataprepper.typeconverter;

 import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
 
 public class LongConverterTests {
     @ParameterizedTest
     @ValueSource(strings = {"1234", "5678"})
     void testStringToLongConversion(String stringValue) {
         LongConverter converter = new LongConverter();
         assertThat(converter.convert(stringValue), equalTo(Long.parseLong(stringValue)));
     }
     @ParameterizedTest
     @ValueSource(floats = {(float)1234.56789, Float.MAX_VALUE, Float.MIN_VALUE})
     void testfloatToLongConversion(float floatValue) {
         LongConverter converter = new LongConverter();
         assertThat(converter.convert(floatValue), equalTo((long)(float)floatValue));
     }
     @ParameterizedTest
     @ValueSource(doubles = {12345678.12345678, 2.0 * Integer.MAX_VALUE, Double.MAX_VALUE, Double.MIN_VALUE})
     void testDoubleToLongConversion(double doubleValue) {
        LongConverter converter = new LongConverter();
        assertThat(converter.convert(doubleValue), equalTo((long)(double)doubleValue));
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
     @Test
     void testInvalidStringConversion() {
         LongConverter converter = new LongConverter();
         assertThrows(IllegalArgumentException.class, () -> converter.convert(new Object()));
     }
 }
 