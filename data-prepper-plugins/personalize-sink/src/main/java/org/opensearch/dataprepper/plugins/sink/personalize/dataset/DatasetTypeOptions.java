/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.personalize.dataset;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines all the dataset types enumerations.
 */
public enum DatasetTypeOptions {
    USERS("users"),
    ITEMS("items"),
    INTERACTIONS("interactions");

    private final String option;
    private static final Map<String, DatasetTypeOptions> OPTIONS_MAP = Arrays.stream(DatasetTypeOptions.values())
            .collect(Collectors.toMap(value -> value.option, value -> value));

    DatasetTypeOptions(final String option) {
        this.option = option.toLowerCase();
    }

    @JsonCreator
    static DatasetTypeOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}