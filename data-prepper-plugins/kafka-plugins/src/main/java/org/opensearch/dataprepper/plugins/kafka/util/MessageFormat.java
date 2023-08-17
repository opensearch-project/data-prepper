/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An enum class which helps to identify the different message formats for the
 * schema type.
 */
public enum MessageFormat {

    PLAINTEXT("plaintext"), JSON("json"), AVRO("avro");

    private static final Map<String, MessageFormat> MESSAGE_FORMAT_MAP = Arrays.stream(MessageFormat.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    MessageFormat(final String type) {
        this.type = type;
    }

    @JsonCreator
    public static MessageFormat getByMessageFormatByName(final String name) {
        return MESSAGE_FORMAT_MAP.get(name.toLowerCase());
    }
}
