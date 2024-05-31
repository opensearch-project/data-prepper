/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

public interface TypeConverter<T> {
  T convert(Object source, ConverterArguments arguments);
}
