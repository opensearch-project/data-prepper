/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum MetricTimestampSource {
    ARRIVAL_TIME("arrival_time"),
    SPAN_END_TIME("span_end_time");

    private static final Map<String, MetricTimestampSource> OPTIONS_MAP = Arrays.stream(MetricTimestampSource.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    MetricTimestampSource(final String option) {
        this.option = option;
    }

    public String getOption() {
        return option;
    }

    @JsonCreator
    public static MetricTimestampSource fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
