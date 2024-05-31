/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

 package org.opensearch.dataprepper.typeconverter;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Converter class for BigDecimal data type. By default, it applies zero scaling keeping the original value as it is.
 * If required, the scale can be set using the setScale method.
 */
public class BigDecimalConverter implements TypeConverter<BigDecimal> {

    public BigDecimal convert(Object source) throws IllegalArgumentException {
        return this.convert(source, 0);
    }

    public BigDecimal convert(Object source, ConverterArguments arguments) throws IllegalArgumentException {
        return this.convert(source, arguments.getScale());
    }

    public BigDecimal convert(Object source, int scale) throws IllegalArgumentException {
        BigDecimal result = null;
        if (source instanceof String) {
            result = new BigDecimal((String)source);
        }
        else if (source instanceof Float) {
            result = BigDecimal.valueOf((Float)source);
        }
        else if (source instanceof Double) {
            result = BigDecimal.valueOf((Double)source);
        }
        else if (source instanceof Boolean) {
            result = ((Boolean)source) ? BigDecimal.valueOf(1L) : BigDecimal.valueOf(0L);
        }
        else if (source instanceof Integer) {
            result = BigDecimal.valueOf((Integer)source);
        }
        else if (source instanceof Long) {
            result = BigDecimal.valueOf((Long)source);
        }
        else if (source instanceof BigDecimal) {
            result = ((BigDecimal)source);
        }

        if(result!=null) {
            if(scale!=0) {
                result = result.setScale(scale, RoundingMode.HALF_EVEN);
            }
            return result;
        }
        throw new IllegalArgumentException("Unsupported type conversion. From Source class: " + source.getClass() + " to BigDecimal");
    }
}
 