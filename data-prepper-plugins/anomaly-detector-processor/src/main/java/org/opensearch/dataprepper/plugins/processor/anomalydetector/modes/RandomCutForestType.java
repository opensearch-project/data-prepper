/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector.modes;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum RandomCutForestType {
    METRICS("metrics");

    private static final Map<String, RandomCutForestType> TYPES_MAP = Arrays.stream(RandomCutForestType.values())
        .collect(Collectors.toMap(
                value -> value.name,
                value -> value
        ));

    private final String name;

    RandomCutForestType(String name) {
        this.name = name.toLowerCase();
    }

    @Override
    public String toString() {
        return name;
    }

    @JsonCreator
    static RandomCutForestType fromOptionValue(final String option) {
        return TYPES_MAP.get(option.toLowerCase());
    }
}
