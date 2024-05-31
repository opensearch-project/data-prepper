/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public class StringConverter implements TypeConverter<String> {

    public String convert(Object source, ConverterArguments arguments) throws IllegalArgumentException {
        return this.convert(source);
    }

    public String convert(Object source) throws IllegalArgumentException {
        if (source instanceof Number || source instanceof Boolean || source instanceof String)  {
            return source.toString();
        }
        throw new IllegalArgumentException("Unsupported type conversion. Source class: " + source.getClass());
    }
}
