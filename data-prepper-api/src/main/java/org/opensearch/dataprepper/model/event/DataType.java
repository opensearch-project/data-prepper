/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents data types that are core to Data Prepper.
 *
 * @since 2.8
 */
public enum DataType {
    /**
     * Type of <i>string</i>. Compatible with {@link java.lang.String}.
     *
     * @since 2.8
     */
    STRING("string"),
    /**
     * Type of <i>boolean</i>. Compatible with the Java <b>boolean</b> primitive data type.
     *
     * @since 2.8
     */
    BOOLEAN("boolean"),
    /**
     * Type of <i>integer</i>. A 32-bit signed integer. Compatible with the Java <b>int</b> primitive data type.
     *
     * @since 2.8
     */
    INTEGER("integer"),
    /**
     * Type of <i>long</i>. A 64-bit signed integer. Compatible with the Java <b>long</b> primitive data type.
     *
     * @since 2.8
     */
    LONG("long"),
    /**
     * Type of <i>double</i>. A 64-bit IEEE 754 floating point number. Compatible with the Java <b>double</b> primitive data type.
     *
     * @since 2.8
     */
    DOUBLE("double"),

    /**
     * Type of <i>BigDecimal</i>. No precision loss possible type. Compatible with the Java <b>BigDecimal</b> primitive data type.
     *
     * @since 2.8
     */
    BIGDECIMAL("bigdecimal"),

    /**
     * Type of <i>map</i>. Compatible with the Java <b>map</b> primitive data type.
     *
     * @since 2.8
     */
    MAP("map"),

    /**
     * Type of <i>array</i>. Compatible with the Java <b>array</b> primitive data type.
     *
     * @since 2.8
     */
    ARRAY("array");

    private static final Map<String, DataType> TYPES_MAP = Arrays.stream(DataType.values())
            .collect(Collectors.toMap(
                    value -> value.typeName,
                    value -> value
            ));

    private final String typeName;

    DataType(final String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets the name of the type. This is the name that users of
     * Data Prepper use or see.
     *
     * @return The name of the type.
     * @since 2.8
     */
    public String getTypeName() {
        return typeName;
    }

    @JsonCreator
    static DataType fromTypeName(final String option) {
        return TYPES_MAP.get(option);
    }

    public static boolean isSameType(final Object object, final String option) {
        DataType type = fromTypeName(option);
        if (type == null)
            throw new IllegalArgumentException("Unknown DataType");
        switch (type) {
            case MAP:
                return (object instanceof Map);
            case ARRAY:
                return (object instanceof ArrayList || object.getClass().isArray());
            case DOUBLE:
                return (object instanceof Double);
            case BOOLEAN:
                return (object instanceof Boolean);
            case INTEGER:
                return (object instanceof Integer);
            case LONG:
                return (object instanceof Long);
            case BIGDECIMAL:
                return (object instanceof BigDecimal);
            default: // STRING
                return (object instanceof String);
        }
    }
}
