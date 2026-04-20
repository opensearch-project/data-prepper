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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.temporal.ChronoUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class MetricTimestampGranularityTest {

    @Test
    void seconds_has_correct_option_value() {
        assertThat(MetricTimestampGranularity.fromOptionValue("seconds"), equalTo(MetricTimestampGranularity.SECONDS));
    }

    @Test
    void minutes_has_correct_option_value() {
        assertThat(MetricTimestampGranularity.fromOptionValue("minutes"), equalTo(MetricTimestampGranularity.MINUTES));
    }

    @Test
    void invalid_option_value_returns_null() {
        assertThat(MetricTimestampGranularity.fromOptionValue("invalid"), nullValue());
    }

    @Test
    void seconds_has_correct_chrono_unit() {
        assertThat(MetricTimestampGranularity.SECONDS.getChronoUnit(), equalTo(ChronoUnit.SECONDS));
    }

    @Test
    void minutes_has_correct_chrono_unit() {
        assertThat(MetricTimestampGranularity.MINUTES.getChronoUnit(), equalTo(ChronoUnit.MINUTES));
    }

    @ParameterizedTest
    @EnumSource(MetricTimestampGranularity.class)
    void all_enum_values_can_be_created_from_option(final MetricTimestampGranularity granularity) {
        assertThat(MetricTimestampGranularity.fromOptionValue(granularity.getOption()), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(MetricTimestampGranularity.class)
    void all_enum_values_have_chrono_unit(final MetricTimestampGranularity granularity) {
        assertThat(granularity.getChronoUnit(), notNullValue());
    }
}
