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

import java.util.Map;

public enum MessageType {
    BEGIN('B'),
    RELATION('R'),
    INSERT('I'),
    UPDATE('U'),
    DELETE('D'),
    COMMIT('C'),
    TYPE('Y');

    private final char value;

    private static final Map<Character, MessageType> MESSAGE_TYPE_MAP = Map.of(
            BEGIN.getValue(), BEGIN,
            RELATION.getValue(), RELATION,
            INSERT.getValue(), INSERT,
            UPDATE.getValue(), UPDATE,
            DELETE.getValue(), DELETE,
            COMMIT.getValue(), COMMIT,
            TYPE.getValue(), TYPE
    );

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
