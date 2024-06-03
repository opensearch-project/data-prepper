/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class BooleanConverter implements TypeConverter<Boolean> {

    public Boolean convert(Object source, ConverterArguments arguments) throws IllegalArgumentException {
        return this.convert(source);
    }

    public Boolean convert(Object source) throws IllegalArgumentException {
        if (source instanceof String) {
            return Boolean.parseBoolean((String)source);
        }
        if (source instanceof Number) {
            Number number = (Number)source;
            return ((number instanceof Integer && number.intValue() != 0) ||
                    (number instanceof Long && number.longValue() != 0) ||
                    (number instanceof Short && number.shortValue() != 0) ||
                    (number instanceof Byte && number.byteValue() != 0)) ||
                    (number.floatValue() != 0) ||
                    (number.doubleValue() != 0);
        }
        if (source instanceof Boolean) {
            return (Boolean)source;
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
