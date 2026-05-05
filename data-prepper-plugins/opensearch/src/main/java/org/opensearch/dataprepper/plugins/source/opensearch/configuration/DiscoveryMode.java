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

/**
 * Controls how the OpenSearch source discovers indices.
 * <p>
 * {@code periodic} (default) re-runs discovery on the configured scheduling interval, allowing newly
 * created indices to be picked up and existing indices to be re-ingested.
 * <p>
 * {@code single_scan} runs discovery exactly once. Once an index has been discovered and processed it
 * is not re-scheduled. This avoids re-ingesting the same indices from the start in long-running
 * pipelines where source coordinator state (e.g. DynamoDB item TTL) could otherwise be lost.
 */
public enum DiscoveryMode {
    PERIODIC("periodic"),
    SINGLE_SCAN("single_scan");

    private static final Map<String, DiscoveryMode> NAMES_MAP = Arrays.stream(DiscoveryMode.values())
            .collect(Collectors.toMap(
                    value -> value.optionName,
                    value -> value
            ));

    private final String optionName;

    DiscoveryMode(final String optionName) {
        this.optionName = optionName;
    }

    @JsonValue
    public String getOptionName() {
        return optionName;
    }

    @JsonCreator
    public static DiscoveryMode fromOptionName(final String optionName) {
        return NAMES_MAP.get(optionName);
    }
}
