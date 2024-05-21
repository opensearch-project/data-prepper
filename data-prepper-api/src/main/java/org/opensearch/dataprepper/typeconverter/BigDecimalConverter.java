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
    //TODO: Scale declared here becomes the state of this converter which
    // makes it globally applied for this pipeline right from the first use, which may not be right.
    // Need to think of a better way to handle this.
    private int scale = 0;

    public void setScale(int scale) {
        this.scale = scale;
    }

    public BigDecimal convert(Object source) throws IllegalArgumentException {
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
 