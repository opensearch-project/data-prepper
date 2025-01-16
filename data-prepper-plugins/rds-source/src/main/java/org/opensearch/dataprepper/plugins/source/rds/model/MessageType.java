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

public enum MessageType {
    BEGIN('B'),
    RELATION('R'),
    INSERT('I'),
    UPDATE('U'),
    DELETE('D'),
    COMMIT('C');

    private final char value;

    MessageType(char value) {
        this.value = value;
    }

    public char getValue() {
        return value;
    }
}
