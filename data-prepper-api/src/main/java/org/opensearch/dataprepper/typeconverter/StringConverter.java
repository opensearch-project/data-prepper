/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
