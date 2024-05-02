/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.opensearch.dataprepper.model.event.Type;
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
    INTEGER(Type.INTEGER, new IntegerConverter()),
    STRING(Type.STRING, new StringConverter()),
    DOUBLE(Type.DOUBLE, new DoubleConverter()),
    BOOLEAN(Type.BOOLEAN, new BooleanConverter()),
    LONG(Type.LONG, new LongConverter());

    private static final Map<String, TargetType> OPTIONS_MAP = Arrays.stream(TargetType.values())
            .collect(Collectors.toMap(
                    value -> value.type.getTypeName(),
                    value -> value
            ));

    private final Type type;
    private final TypeConverter<?> targetConverter;

    TargetType(final Type type, final TypeConverter<?> target) {
        this.type = type;
        this.targetConverter = target;
    }

    public TypeConverter<?> getTargetConverter() {
        return targetConverter;
    }

    Type getDataType() {
        return type;
    }

    @JsonCreator
    static TargetType fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
