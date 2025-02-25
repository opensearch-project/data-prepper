/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPrepperDurationParserTest {

    @Test
    void nullValueThrowIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> DataPrepperDurationParser.parse(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"6s1s", "60ms 100s", "20.345s", "-1s", "06s", "100m", "100sm", "100"})
    void invalidDurationStringsThrowIllegalArgumentException(final String durationString) {
        assertThrows(IllegalArgumentException.class, () -> DataPrepperDurationParser.parse(durationString));
    }

    @Test
    void ISO_8601_duration_string_returns_correct_duration() {
        final String durationString = "PT15M";
        final Duration result = DataPrepperDurationParser.parse(durationString);
        assertThat(result, equalTo(Duration.ofMinutes(15)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0s", "0ms"})
    void simple_duration_strings_of_0_return_correct_duration(final String durationString) {
        final Duration result = DataPrepperDurationParser.parse(durationString);

        assertThat(result, equalTo(Duration.ofSeconds(0)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"60s", "60000ms", "60 s", "60000 ms", "  60  s "})
    void simple_duration_strings_of_60_seconds_return_correct_duration(final String durationString) {
        final Duration result = DataPrepperDurationParser.parse(durationString);

        assertThat(result, equalTo(Duration.ofSeconds(60)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"5s", "5000ms", "5 s", "5000 ms", "  5  s "})
    void simple_duration_strings_of_5_seconds_return_correct_duration(final String durationString) {
        final Duration result = DataPrepperDurationParser.parse(durationString);

        assertThat(result, equalTo(Duration.ofSeconds(5)));
    }
}