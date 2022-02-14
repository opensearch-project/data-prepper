/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.date;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class DateProcessorConfigTest {
    @Test
    void testDefaultConfig() {
        final DateProcessorConfig dateProcessorConfig = new DateProcessorConfig();

        assertThat(dateProcessorConfig.getFromTimeReceived(), equalTo(DateProcessorConfig.DEFAULT_FROM_TIME_RECEIVED));
        assertThat(dateProcessorConfig.getDestination(), equalTo(DateProcessorConfig.DEFAULT_DESTINATION));
    }

    @Nested
    class Validation {

        private String random;
        final DateProcessorConfig dateProcessorConfig = new DateProcessorConfig();

        @BeforeEach
        void setUp() {
            random = UUID.randomUUID().toString();
        }

        @Test
        void isValidMatchAndFromTimestampReceived_should_return_true_if_from_time_received_is_true() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(dateProcessorConfig, "fromTimeReceived", true);
            assertThat(dateProcessorConfig.isValidMatchAndFromTimestampReceived(), equalTo(true));
        }

        @Test
        void isValidMatchAndFromTimestampReceived_should_return_false_if_from_time_received_and_match_are_not_configured() {
            assertThat(dateProcessorConfig.isValidMatchAndFromTimestampReceived(), equalTo(false));
        }

        @Test
        void isValidMatchAndFromTimestampReceived_should_return_false_if_from_time_received_and_match_are_configured() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();
            match.put(UUID.randomUUID().toString(), Collections.singletonList(random));
            reflectivelySetField(dateProcessorConfig, "fromTimeReceived", true);
            reflectivelySetField(dateProcessorConfig, "match", match);

            assertThat(dateProcessorConfig.isValidMatchAndFromTimestampReceived(), equalTo(false));
        }

        @Test
        void isValidMatch_should_return_true_if_match_has_single_entry() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();
            match.put(UUID.randomUUID().toString(), Collections.singletonList(random));

            reflectivelySetField(dateProcessorConfig, "match", match);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(true));
        }

        @Test
        void isValidMatch_should_return_false_if_match_has_multiple_entries() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();
            match.put(UUID.randomUUID().toString(), Collections.singletonList(random));
            match.put(UUID.randomUUID().toString(), Collections.singletonList(random));

            reflectivelySetField(dateProcessorConfig, "match", match);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(false));
        }

        @Test
        void isValidMatch_should_return_true_if_match_is_null() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(dateProcessorConfig, "match", null);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(true));
        }

        @Test
        void isValidMatch_should_return_false_if_match_is_empty() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();

            reflectivelySetField(dateProcessorConfig, "match", match);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(false));
        }

        @Test
        void isValidMatch_should_return_false_if_match_has_zero_patterns() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();
            match.put(UUID.randomUUID().toString(), Collections.emptyList());

            reflectivelySetField(dateProcessorConfig, "match", match);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(false));
        }

        @Test
        void isValidMatch_should_return_true_if_match_has_at_least_one_pattern() throws NoSuchFieldException, IllegalAccessException {
            HashMap<String, List<String>> match = new HashMap<>();
            match.put(UUID.randomUUID().toString(), Collections.singletonList(random));

            reflectivelySetField(dateProcessorConfig, "match", match);
            assertThat(dateProcessorConfig.isValidMatch(), equalTo(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"America/New_York", "America/Los_Angeles", "Australia/Adelaide", "Japan"})
        void isTimezoneValid_should_return_true_if_timezone_is_valid(String timezone) throws NoSuchFieldException, IllegalAccessException {

            reflectivelySetField(dateProcessorConfig, "timezone", timezone);
            assertThat(dateProcessorConfig.isTimezoneValid(), equalTo(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"invalidZone", "randomZone"})
        void isTimezoneValid_should_return_false_if_timezone_is_invalid(String timezone) throws NoSuchFieldException, IllegalAccessException {

            reflectivelySetField(dateProcessorConfig, "timezone", timezone);
            assertThat(dateProcessorConfig.isTimezoneValid(), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(strings = {"en-US", "zh-CN", "fr_FR", "ja_JP"})
        void isLocaleValid_should_return_true_if_locale_is_valid(String locale) throws NoSuchFieldException, IllegalAccessException {

            reflectivelySetField(dateProcessorConfig, "locale", locale);
            assertThat(dateProcessorConfig.isTimezoneValid(), equalTo(true));
        }

        @ParameterizedTest
        @ValueSource(strings = {"enUS", "en_US_TX_VAR"})
        void isLocaleValid_should_return_false_if_locale_is_invalid(String locale) throws NoSuchFieldException, IllegalAccessException {

            reflectivelySetField(dateProcessorConfig, "locale", locale);
            assertThat(dateProcessorConfig.isLocaleValid(), equalTo(false));
        }
        @ParameterizedTest
        @ValueSource(strings = {"root", "Root", "ROOT"})
        void isLocaleValid_should_return_true_if_locale_is_root(String locale) throws NoSuchFieldException, IllegalAccessException {

            reflectivelySetField(dateProcessorConfig, "locale", locale);
            assertThat(dateProcessorConfig.isLocaleValid(), equalTo(true));
        }
    }

    private void reflectivelySetField(final DateProcessorConfig dateProcessorConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = DateProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(dateProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}