/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class StringConverter implements TypeConverter<String> {
    public String convert(Object source) throws IllegalArgumentException {
        if (source instanceof Long) {
            return Long.toString(((Number)source).longValue());
        }
        if (source instanceof Double) {
            return Double.toString((Double)source);
        }
        if (source instanceof Float) {
            return Float.toString((Float)source);
        }
        if (source instanceof Number) {
            return Integer.toString(((Number)source).intValue());
        }
        if (source instanceof Boolean) {
            return Boolean.toString((Boolean)source);
        }
        if (source instanceof String) {
            return (String)source;
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
