/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class BooleanConverter implements TypeConverter<Boolean> {
    public Boolean convert(Object source) throws IllegalArgumentException {
        if (source instanceof String) {
            return Boolean.parseBoolean((String)source);
        }
        if (source instanceof Number) {
            Number number = (Number)source;
            return ((number.intValue() != 0) ||
                    (number.longValue() != 0) ||
                    (number.floatValue() != 0) ||
                    (number.doubleValue() != 0) ||
                    (number.shortValue() != 0) ||
                    (number.byteValue() != 0)) ? true : false;
        }
        if (source instanceof Boolean) {
            return (Boolean)source;
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
