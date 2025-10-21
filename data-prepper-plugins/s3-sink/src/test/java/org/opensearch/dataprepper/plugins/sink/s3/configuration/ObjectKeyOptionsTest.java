/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.model.codec.OutputCodec.objectMapper;

class ObjectKeyOptionsTest {

    private static final String DEFAULT_FILE_PATTERN = "events-%{yyyy-MM-dd'T'HH-mm-ss'Z'}";

    @Test
    void default_file_pattern_test() {
        assertThat(new ObjectKeyOptions().getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void default_path_prefix_test() {
        assertThat(new ObjectKeyOptions().getPathPrefix(), equalTo(null));
    }

    @Test
    void getNamePattern_returns_custom_pattern_when_set() throws JsonProcessingException {
        String customPattern = "custom-events-%{yyyy-MM-dd}";
        String json = String.format("{\"name_pattern\": \"%s\"}", customPattern);
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePattern(), equalTo(customPattern));
    }

    @Test
    void getNamePattern_returns_default_when_set_to_empty_string() throws JsonProcessingException {
        String json = "{\"name_pattern\": \"\"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void getNamePattern_returns_default_when_set_to_whitespace() throws JsonProcessingException {
        String json = "{\"name_pattern\": \"   \"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }

    @Test
    void getNamePattern_returns_default_when_set_to_tabs_and_spaces() throws JsonProcessingException {
        String json = "{\"name_pattern\": \" \\t \\n \"}";
        ObjectKeyOptions options = objectMapper.readValue(json, ObjectKeyOptions.class);

        assertThat(options.getNamePattern(), equalTo(DEFAULT_FILE_PATTERN));
    }
}