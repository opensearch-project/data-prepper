/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.rds.model;

public enum StreamEventType {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    private final String eventType;

    StreamEventType(final String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return eventType;
    }
}
