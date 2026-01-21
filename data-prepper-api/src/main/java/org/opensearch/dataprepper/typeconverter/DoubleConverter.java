/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.typeconverter;

public class DoubleConverter implements TypeConverter<Double> {

    public Double convert(Object source, ConverterArguments arguments) throws IllegalArgumentException {
        return convert(source);
    }

    public Double convert(Object source) throws IllegalArgumentException {
        if (source instanceof String) {
            return Double.parseDouble((String)source);
        }
        if (source instanceof Double) {
            return (Double)source;
        }
        if (source instanceof Number) {
            return (((Number)source).doubleValue());
        }
        if (source instanceof Boolean) {
            return ((Boolean)source) ? 1.0 : 0.0;
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
