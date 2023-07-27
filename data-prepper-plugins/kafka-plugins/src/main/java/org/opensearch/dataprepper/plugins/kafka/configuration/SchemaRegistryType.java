/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum SchemaRegistryType {
    GLUE("glue"),
    CONFLUENT("confluent");

    private static final Map<String, SchemaRegistryType> OPTIONS_MAP = Arrays.stream(SchemaRegistryType.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    SchemaRegistryType(final String type) {
        this.type = type;
    }

    @JsonCreator
    static SchemaRegistryType fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }
}
