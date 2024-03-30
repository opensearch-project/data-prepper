/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class IntegerConverter implements TypeConverter<Integer> {
    public Integer convert(Object source) throws IllegalArgumentException {
        if (source instanceof String) {
            return Integer.parseInt((String)source);
        }
        if (source instanceof Float) {
            return (int)(float)((Float)source);
        }
        if (source instanceof Boolean) {
            return ((Boolean)source) ? 1 : 0;
        }
        if (source instanceof Integer) {
            return (Integer)source;
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
