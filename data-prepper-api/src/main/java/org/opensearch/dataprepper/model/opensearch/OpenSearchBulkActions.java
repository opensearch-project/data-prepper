/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.opensearch;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum OpenSearchBulkActions {

    CREATE("create"),
    UPSERT("upsert"),
    UPDATE("update"),
    DELETE("delete"),
    INDEX("index");

    private static final Map<String, OpenSearchBulkActions> ACTIONS_MAP = Arrays.stream(OpenSearchBulkActions.values())
            .collect(Collectors.toMap(
                    value -> value.action,
                    value -> value
            ));

    private final String action;

    OpenSearchBulkActions(String action) {
        this.action = action.toLowerCase();
    }

    @Override
    public String toString() {
        return action;
    }

    @JsonCreator
    public static OpenSearchBulkActions fromOptionValue(final String option) {
        return ACTIONS_MAP.get(option.toLowerCase());
    }

}
