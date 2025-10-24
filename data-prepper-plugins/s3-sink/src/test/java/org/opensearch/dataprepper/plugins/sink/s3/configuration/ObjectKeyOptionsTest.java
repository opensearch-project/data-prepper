/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.model.codec.OutputCodec.objectMapper;

class ObjectKeyOptionsTest {

    private static final String DEFAULT_FILE_PATTERN = "events-%{yyyy-MM-dd'T'HH-mm-ss'Z'}";
    private static final String DEFAULT_TIME_PATTERN = "%{yyyy-MM-dd'T'HH-mm-ss'Z'}";

    @Test
    void default_file_pattern_test() {
        assertThat(new ObjectKeyOptions().getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void default_path_prefix_test() {
        assertThat(new ObjectKeyOptions().getPathPrefix(), equalTo(null));
    }

    @Test
    void default_name_pattern_prefix_test() {
        assertThat(new ObjectKeyOptions().getNamePatternPrefix(), equalTo(null));
    }

    @Test
    void getNamePatternPrefix_returns_configured_value() throws JsonProcessingException {
        String prefix = "my-custom-events";
        String json = String.format("{\"name_pattern_prefix\": \"%s\"}", prefix);
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePatternPrefix(), equalTo(prefix));
    }

    @Test
    void getNamePattern_returns_default_when_prefix_is_null() {
        ObjectKeyOptions options = new ObjectKeyOptions();

        assertThat(options.getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void getNamePattern_returns_prefix_with_default_time_pattern_when_prefix_is_set() throws JsonProcessingException {
        String prefix = "custom-events";
        String json = String.format("{\"name_pattern_prefix\": \"%s\"}", prefix);
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        String expected = prefix + "-" + DEFAULT_TIME_PATTERN;
        assertThat(options.getNamePattern(), equalTo(expected));
    }

    @Test
    void getNamePattern_appends_default_time_pattern_correctly() throws JsonProcessingException {
        String prefix = "logs";
        String json = String.format("{\"name_pattern_prefix\": \"%s\"}", prefix);
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePattern(), equalTo("logs-%{yyyy-MM-dd'T'HH-mm-ss'Z'}"));
    }

    @Test
    void isTimePatternExcludedFromNamePatternPrefix_returns_true_when_prefix_is_empty() throws JsonProcessingException {
        String json = "{\"name_pattern_prefix\": \"\"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertTrue(options.isTimePatternExcludedFromNamePatternPrefix());
    }

    @Test
    void isTimePatternExcludedFromNamePatternPrefix_returns_true_for_prefix_with_hyphens() throws JsonProcessingException {
        String json = "{\"name_pattern_prefix\": \"my-app-logs\"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertTrue(options.isTimePatternExcludedFromNamePatternPrefix());
    }

    @Test
    void isTimePatternExcludedFromNamePatternPrefix_returns_false_when_contains_full_datetime_pattern() throws JsonProcessingException {
        String json = "{\"name_pattern_prefix\": \"events-%{yyyy-MM-dd'T'HH-mm-ss'Z'}\"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertFalse(options.isTimePatternExcludedFromNamePatternPrefix());
    }
}