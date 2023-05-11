/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum WildCardOptions {
    OPEN("open"),
    CLOSED("closed"),
    NONE("none"),
    ALL("all");
    private static final Map<String, WildCardOptions> OPTIONS_MAP = Arrays.stream(WildCardOptions.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    WildCardOptions(final String option) {
        this.option = option;
    }

    @JsonCreator
    static WildCardOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
