/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import java.math.BigDecimal;
public class IntegerConverter implements TypeConverter<Integer> {

    public Integer convert(Object source, ConverterArguments arguments) throws IllegalArgumentException {
        return convert(source);
    }

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
        if (source instanceof BigDecimal) {
            return ((BigDecimal)source).intValue();
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
