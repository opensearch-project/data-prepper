/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.dataprepper.typeconverter;

 public class LongConverter implements TypeConverter<Long> {
     public Long convert(Object source) throws IllegalArgumentException {
         if (source instanceof String) {
             return Long.parseLong((String)source);
         }
         if (source instanceof Float) {
             return (long)(float)((Float)source);
         }
         if (source instanceof Double) {
             return (long)(double)((Double)source);
         }
         if (source instanceof Boolean) {
             return ((Boolean)source) ? 1L : 0L;
         }
         if (source instanceof Integer) {
             return ((Integer)source).longValue();
         }
         if (source instanceof Long) {
             return (Long)source;
         }
         throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
     }
 }
 