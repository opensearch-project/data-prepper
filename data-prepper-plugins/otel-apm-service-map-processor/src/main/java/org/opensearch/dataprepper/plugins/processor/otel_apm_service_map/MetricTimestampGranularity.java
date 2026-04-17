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

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum MetricTimestampGranularity {
    SECONDS("seconds", ChronoUnit.SECONDS),
    MINUTES("minutes", ChronoUnit.MINUTES);

    private static final Map<String, MetricTimestampGranularity> OPTIONS_MAP = Arrays.stream(MetricTimestampGranularity.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;
    private final ChronoUnit chronoUnit;

    MetricTimestampGranularity(final String option, final ChronoUnit chronoUnit) {
        this.option = option;
        this.chronoUnit = chronoUnit;
    }

    public String getOption() {
        return option;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    @JsonCreator
    public static MetricTimestampGranularity fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
