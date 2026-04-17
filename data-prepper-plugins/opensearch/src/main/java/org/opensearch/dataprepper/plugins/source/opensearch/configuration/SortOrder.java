/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum SortOrder {
    ASCENDING("ascending", "asc"),
    DESCENDING("descending", "desc");

    private static final Map<String, SortOrder> NAMES_MAP = Arrays.stream(SortOrder.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));

    private final String optionName;
    private final String sortOrderValue;

    SortOrder(final String optionName, final String sortOrderValue) {
        this.optionName = optionName;
        this.sortOrderValue = sortOrderValue;
    }

    @JsonValue
    public String getOptionName() {
        return optionName;
    }

    public String getSortOrderValue() {
        return sortOrderValue;
    }

    @JsonCreator
    public static SortOrder fromOptionName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}