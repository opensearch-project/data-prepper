/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum KafkaKeyMode {
    DISCARD("discard"),
    INCLUDE_AS_FIELD("include_as_field"),
    INCLUDE_AS_METADATA("include_as_metadata");

    private static final Map<String, KafkaKeyMode> OPTIONS_MAP = Arrays.stream(KafkaKeyMode.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    KafkaKeyMode(final String type) {
        this.type = type;
    }

    @JsonCreator
    static KafkaKeyMode fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }
}

