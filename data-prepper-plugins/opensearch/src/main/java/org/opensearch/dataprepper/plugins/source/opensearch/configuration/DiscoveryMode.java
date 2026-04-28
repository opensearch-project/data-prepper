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

import java.util.Arrays;

/**
 * Controls how the OpenSearch source discovers indices.
 * <p>
 * {@link #PERIODIC} (default) re-runs discovery on the configured scheduling interval, allowing newly
 * created indices to be picked up and existing indices to be re-ingested.
 * <p>
 * {@link #SINGLE_SCAN} runs discovery exactly once. Once an index has been discovered and processed it is
 * not re-scheduled. This avoids re-ingesting the same indices from the start in long-running pipelines
 * where source coordinator state (e.g. DynamoDB item TTL) could otherwise be lost.
 */
public enum DiscoveryMode {
    PERIODIC,
    SINGLE_SCAN;

    @JsonCreator
    public static DiscoveryMode fromString(final String value) {
        return Arrays.stream(values())
                .filter(mode -> mode.name().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        String.format("Invalid discovery_mode '%s'. Supported values are: PERIODIC, SINGLE_SCAN", value)));
    }
}
