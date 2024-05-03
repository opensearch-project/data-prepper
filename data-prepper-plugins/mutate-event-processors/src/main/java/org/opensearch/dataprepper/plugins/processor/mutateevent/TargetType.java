/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.opensearch.dataprepper.model.event.DataType;
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
    INTEGER(DataType.INTEGER, new IntegerConverter()),
    STRING(DataType.STRING, new StringConverter()),
    DOUBLE(DataType.DOUBLE, new DoubleConverter()),
    BOOLEAN(DataType.BOOLEAN, new BooleanConverter()),
    LONG(DataType.LONG, new LongConverter());

    private static final Map<String, TargetType> OPTIONS_MAP = Arrays.stream(TargetType.values())
            .collect(Collectors.toMap(
                    value -> value.dataType.getTypeName(),
                    value -> value
            ));

    private final DataType dataType;
    private final TypeConverter<?> targetConverter;

    TargetType(final DataType dataType, final TypeConverter<?> target) {
        this.dataType = dataType;
        this.targetConverter = target;
    }

    public TypeConverter<?> getTargetConverter() {
        return targetConverter;
    }

    DataType getDataType() {
        return dataType;
    }

    @JsonCreator
    static TargetType fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option.toLowerCase());
    }
}
