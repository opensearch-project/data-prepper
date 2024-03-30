/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.opensearch.dataprepper.typeconverter.TypeConverter;
import org.opensearch.dataprepper.typeconverter.IntegerConverter;
import org.opensearch.dataprepper.typeconverter.StringConverter;
import org.opensearch.dataprepper.typeconverter.DoubleConverter;
import org.opensearch.dataprepper.typeconverter.BooleanConverter;
import org.opensearch.dataprepper.typeconverter.LongConverter;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum TargetType {
    INTEGER("integer", new IntegerConverter()),
    STRING("string", new StringConverter()),
    DOUBLE("double", new DoubleConverter()),
    BOOLEAN("boolean", new BooleanConverter()),
    LONG("long", new LongConverter());

    private static final Map<String, TargetType> OPTIONS_MAP = Arrays.stream(TargetType.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;
    private final TypeConverter targetConverter;

    TargetType(final String option, final TypeConverter target) {
        this.option = option;
        this.targetConverter = target;
    }

    public TypeConverter getTargetConverter() {
        return targetConverter;
    }
    
    @JsonCreator
    static TargetType fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
