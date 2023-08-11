/*
 * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.dataprepper.plugins.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThrows;
import static org.opensearch.dataprepper.plugins.source.CustomLocalDateTimeDeserializer.CURRENT_LOCAL_DATE_TIME_STRING;

class CustomLocalDateTimeDeserializerTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(LocalDateTime.class, new CustomLocalDateTimeDeserializer());
        objectMapper.registerModule(simpleModule);
    }

    @ParameterizedTest
    @ValueSource(strings = {"2023-01-2118:00:00", "2023-01-21T8:00:00"})
    void deserialize_with_invalid_values_throws(final String invalidDateTimeString) {
        assertThrows(IllegalArgumentException.class, () -> objectMapper.convertValue(invalidDateTimeString, LocalDateTime.class));
    }

    @Test
    void deserialize_with_predefined_custom_value_returns_current_local_datetime() {
        final LocalDateTime expectedDateTime = objectMapper.convertValue(CURRENT_LOCAL_DATE_TIME_STRING, LocalDateTime.class);
        assertThat(expectedDateTime, lessThan(LocalDateTime.now()));
        assertThat(expectedDateTime, greaterThan(LocalDateTime.now().minus(Duration.of(5, ChronoUnit.SECONDS))));
    }

    @Test
    void deserialize_with_iso_local_date_time_string_returns_correct_local_datetime() {
        final String testLocalDateTimeString = "2023-01-21T18:30:45";
        final LocalDateTime expectedDateTime = objectMapper.convertValue(testLocalDateTimeString, LocalDateTime.class);
        assertThat(expectedDateTime, equalTo(LocalDateTime.of(2023, 1, 21, 18, 30, 45)));
        assertThat(expectedDateTime.getYear(), equalTo(2023));
        assertThat(expectedDateTime.getMonthValue(), equalTo(1));
        assertThat(expectedDateTime.getDayOfMonth(), equalTo(21));
        assertThat(expectedDateTime.getHour(), equalTo(18));
        assertThat(expectedDateTime.getMinute(), equalTo(30));
        assertThat(expectedDateTime.getSecond(), equalTo(45));
    }
}