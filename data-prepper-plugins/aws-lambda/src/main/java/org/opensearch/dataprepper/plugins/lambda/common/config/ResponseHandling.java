/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum representing response handling strategies for Lambda streaming responses.
 */
public enum ResponseHandling {
    RECONSTRUCT_DOCUMENT("reconstruct-document");

    private static final Map<String, ResponseHandling> NAMES_MAP = Arrays.stream(ResponseHandling.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));

    private final String optionName;

    ResponseHandling(final String optionName) {
        this.optionName = optionName;
    }

    @JsonValue
    public String getOptionName() {
        return optionName;
    }

    @JsonCreator
    public static ResponseHandling fromOptionName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}
