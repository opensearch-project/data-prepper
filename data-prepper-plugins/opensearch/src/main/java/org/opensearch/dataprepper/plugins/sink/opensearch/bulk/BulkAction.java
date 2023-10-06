/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum BulkAction {

    CREATE("create"),
    UPSERT("upsert"),
    UPDATE("update"),
    DELETE("delete"),
    INDEX("index");

    private static final Map<String, BulkAction> ACTIONS_MAP = Arrays.stream(BulkAction.values())
        .collect(Collectors.toMap(
                value -> value.action,
                value -> value
        ));

    private final String action;

    BulkAction(String action) {
        this.action = action.toLowerCase();
    }

    @Override
    public String toString() {
        return action;
    }

    @JsonCreator
    public static BulkAction fromOptionValue(final String option) {
        return ACTIONS_MAP.get(option.toLowerCase());
    }

}
