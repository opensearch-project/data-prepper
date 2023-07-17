/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum MskBrokerConnectionType {
    PUBLIC("public"),
    SINGLE_VPC("single_vpc"),
    MULTI_VPC("multi_vpc");

    private static final Map<String, MskBrokerConnectionType> OPTIONS_MAP = Arrays.stream(MskBrokerConnectionType.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    MskBrokerConnectionType(final String type) {
        this.type = type;
    }

    @JsonCreator
    static MskBrokerConnectionType fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }
}
