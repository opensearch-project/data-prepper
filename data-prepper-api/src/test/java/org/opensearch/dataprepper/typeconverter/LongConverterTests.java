/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.dataprepper.typeconverter;

 import org.junit.jupiter.api.Test;
 
 import static org.hamcrest.CoreMatchers.equalTo;
 import static org.hamcrest.MatcherAssert.assertThat;
 import static org.junit.jupiter.api.Assertions.assertThrows;
 
 public class LongConverterTests {
     @Test
     void testStringToLongConversion() {
         LongConverter converter = new LongConverter();
         final String stringConstant = "1234";
         assertThat(converter.convert(stringConstant), equalTo(Long.parseLong(stringConstant)));
     }
     @Test
     void testfloatToLongConversion() {
         LongConverter converter = new LongConverter();
         final Float floatConstant = (float)1234.56789;
         assertThat(converter.convert(floatConstant), equalTo((long)(float)floatConstant));
     }
     @Test
     void testDoubleToLongConversion() {
        LongConverter converter = new LongConverter();
        final Double doubleConstant = (double)12345678.12345678;
        assertThat(converter.convert(doubleConstant), equalTo((long)(double)doubleConstant));
     }
     @Test
     void testBooleanToLongConversion() {
         LongConverter converter = new LongConverter();
         final Boolean boolFalseConstant = false;
         assertThat(converter.convert(boolFalseConstant), equalTo(0L));
         final Boolean boolTrueConstant = true;
         assertThat(converter.convert(boolTrueConstant), equalTo(1L));
     }
     @Test
     void testLongToLongConversion() {
         LongConverter converter = new LongConverter();
         final Long longConstant = (long)1234;
         assertThat(converter.convert(longConstant), equalTo(longConstant));
     }
     @Test
     void testInvalidStringConversion() {
         IntegerConverter converter = new IntegerConverter();
         assertThrows(IllegalArgumentException.class, () -> converter.convert(new Object()));
         IntegerConverter converter = new IntegerConverter();
         assertThrows(IllegalArgumentException.class, () -> converter.convert(new Object()));
     }
 }
 