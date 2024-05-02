/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents data types that are core to Data Prepper.
 *
 * @since 2.8
 */
public enum Type {
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
    DOUBLE("double");

    private static final Map<String, Type> TYPES_MAP = Arrays.stream(Type.values())
            .collect(Collectors.toMap(
                    value -> value.typeName,
                    value -> value
            ));

    private final String typeName;

    Type(final String typeName) {
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
    static Type fromTypeName(final String option) {
        return TYPES_MAP.get(option);
    }
}
