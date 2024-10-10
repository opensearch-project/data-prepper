/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum WhitespaceOption {
    LENIENT("lenient"),
    STRICT("strict");

    private static final Map<String, WhitespaceOption> NAMES_MAP = Arrays.stream(WhitespaceOption.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));

    private final String optionName;

    WhitespaceOption(final String optionName) {
        this.optionName = optionName;
    }

    @JsonValue
    public String getWhitespaceName() {
        return optionName;
    }

    @JsonCreator
    public static WhitespaceOption fromWhitespaceName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}
