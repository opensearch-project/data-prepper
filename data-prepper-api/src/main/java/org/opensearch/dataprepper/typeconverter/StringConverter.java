/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.typeconverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

public class StringConverter implements TypeConverter<String> {
    private static final Logger LOG = LoggerFactory.getLogger(StringConverter.class);

    public String convert(final Object source) {
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
        LOG.error(EVENT, "Unable to convert {} to String", source);
        if (Objects.nonNull(source)) {
            return source.toString();
        } else {
            return "";
        }
    }
}
