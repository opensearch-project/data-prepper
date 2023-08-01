/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum HTTPMethodOptions {
    PUT("PUT"),
    POST("POST");

    private static final Map<String, HTTPMethodOptions> OPTIONS_MAP = Arrays.stream(HTTPMethodOptions.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    HTTPMethodOptions(final String option) {
        this.option = option;
    }

    @JsonCreator
    static HTTPMethodOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
