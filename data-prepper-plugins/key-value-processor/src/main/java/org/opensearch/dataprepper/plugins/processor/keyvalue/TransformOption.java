/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum TransformOption {
    NONE("none", key -> key),
    LOWERCASE("lowercase", String::toLowerCase),
    UPPERCASE("uppercase", String::toUpperCase),
    CAPITALIZE("capitalize", key -> key.substring(0, 1).toUpperCase() + key.substring(1));

    private static final Map<String, TransformOption> NAMES_MAP = Arrays.stream(TransformOption.values())
            .collect(Collectors.toMap(
                    value -> value.transformName,
                    value -> value
            ));

    private final String transformName;
    private final Function<String, String> transformFunction;

    TransformOption(final String transformName, final Function<String, String> transformFunction) {
        this.transformName = transformName;
        this.transformFunction = transformFunction;
    }

    @JsonValue
    public String getTransformName() {
        return transformName;
    }

    Function<String, String> getTransformFunction() {
        return transformFunction;
    }

    @JsonCreator
    public static TransformOption fromTransformName(final String transformName) {
        if(Objects.equals(transformName, ""))
            return NONE;
        return NAMES_MAP.get(transformName);
    }
}
