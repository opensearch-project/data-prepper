/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.gcp.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * How a plugin authenticates to Google Cloud.
 */
public enum GcpAuthMode {
    ADC("adc"),
    SERVICE_ACCOUNT_KEY("service_account_key");

    private static final Map<String, GcpAuthMode> OPTIONS_MAP = Arrays.stream(GcpAuthMode.values())
            .collect(Collectors.toMap(GcpAuthMode::getOptionValue, Function.identity()));

    private final String optionValue;

    GcpAuthMode(final String optionValue) {
        this.optionValue = optionValue;
    }

    @JsonValue
    public String getOptionValue() {
        return optionValue;
    }

    @JsonCreator
    public static GcpAuthMode fromOptionValue(final String optionValue) {
        return OPTIONS_MAP.get(optionValue);
    }
}
