/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum MessageType {
    BEGIN('B'),
    COMMIT('C'),
    DELETE('D'),
    INSERT('I'),
    MESSAGE('M'),
    ORIGIN('O'),
    RELATION('R'),
    TRUNCATE('T'),
    TYPE('Y'),
    UPDATE('U');

    private final char value;

    private static final Map<Character, MessageType> MESSAGE_TYPE_MAP = Arrays.stream(values())
            .collect(Collectors.toMap(
                    messageType -> messageType.value,
                    messageType -> messageType
            ));

    MessageType(char value) {
        this.value = value;
    }

    public char getValue() {
        return value;
    }

    public static MessageType from(char value) {
        if (!MESSAGE_TYPE_MAP.containsKey(value)) {
            throw new IllegalArgumentException("Invalid MessageType value: " + value);
        }
        return MESSAGE_TYPE_MAP.get(value);
    }
}
