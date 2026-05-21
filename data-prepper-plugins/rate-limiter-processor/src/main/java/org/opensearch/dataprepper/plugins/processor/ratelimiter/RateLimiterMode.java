/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.ratelimiter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum RateLimiterMode {
    DROP("drop");

    private static final Map<String, RateLimiterMode> MODES_MAP = Arrays.stream(RateLimiterMode.values())
            .collect(Collectors.toMap(
                    value -> value.name,
                    value -> value
            ));

    private final String name;

    RateLimiterMode(String name) {
        this.name = name.toLowerCase();
    }

    @JsonCreator
    static RateLimiterMode fromOptionValue(final String option) {
        return MODES_MAP.get(option.toLowerCase());
    }

    @JsonValue
    public String getOptionValue() {
        return name;
    }
}
