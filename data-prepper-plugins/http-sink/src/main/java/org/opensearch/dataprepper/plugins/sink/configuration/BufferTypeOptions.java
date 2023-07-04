/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines all the buffer types enumerations.
 */
public enum BufferTypeOptions {

    IN_MEMORY("in_memory"),
    LOCAL_FILE("local_file");

    private final String option;

    private static final Map<String, BufferTypeOptions> OPTIONS_MAP = Arrays.stream(BufferTypeOptions.values())
            .collect(Collectors.toMap(value -> value.option, value -> value));

    BufferTypeOptions(final String option) {
        this.option = option.toLowerCase();
    }
    @JsonCreator
    static BufferTypeOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}