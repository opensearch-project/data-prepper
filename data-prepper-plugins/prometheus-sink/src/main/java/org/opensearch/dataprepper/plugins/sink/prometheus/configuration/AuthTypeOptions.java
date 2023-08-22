/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum AuthTypeOptions {
    HTTP_BASIC("http-basic"),
    BEARER_TOKEN("bearer-token"),
    UNAUTHENTICATED("unauthenticated");

    private static final Map<String, AuthTypeOptions> OPTIONS_MAP = Arrays.stream(AuthTypeOptions.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    AuthTypeOptions(final String option) {
        this.option = option;
    }

    @JsonCreator
    static AuthTypeOptions fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
