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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class MetricTimestampSourceTest {

    @Test
    void arrival_time_has_correct_option_value() {
        assertThat(MetricTimestampSource.fromOptionValue("arrival_time"), equalTo(MetricTimestampSource.ARRIVAL_TIME));
    }

    @Test
    void span_end_time_has_correct_option_value() {
        assertThat(MetricTimestampSource.fromOptionValue("span_end_time"), equalTo(MetricTimestampSource.SPAN_END_TIME));
    }

    @Test
    void invalid_option_value_returns_null() {
        assertThat(MetricTimestampSource.fromOptionValue("invalid"), nullValue());
    }

    @ParameterizedTest
    @EnumSource(MetricTimestampSource.class)
    void all_enum_values_can_be_created_from_option(final MetricTimestampSource source) {
        assertThat(MetricTimestampSource.fromOptionValue(source.getOption()), notNullValue());
    }
}
