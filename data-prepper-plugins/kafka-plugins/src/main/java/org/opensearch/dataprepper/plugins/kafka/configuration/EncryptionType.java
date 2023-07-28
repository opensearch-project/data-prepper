/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum EncryptionType {
    NONE("none"),
    SSL("ssl");

    private static final Map<String, EncryptionType> OPTIONS_MAP = Arrays.stream(EncryptionType.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    EncryptionType(final String type) {
        this.type = type;
    }

    @JsonCreator
    static EncryptionType fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }
}
