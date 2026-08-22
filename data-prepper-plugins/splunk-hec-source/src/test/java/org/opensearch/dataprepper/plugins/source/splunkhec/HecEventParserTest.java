/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HecEventParserTest {

    private HecEventParser parser;

    @BeforeEach
    void setUp() {
        parser = new HecEventParser();
    }

    @Test
    void parse_with_null_data_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () -> parser.parse(null));
    }

    @Test
    void parse_single_event() throws IOException {
        final String json = "{\"event\": \"test message\", \"host\": \"web01\"}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        assertThat(result.get(0), hasEntry("event", "test message"));
        assertThat(result.get(0), hasEntry("host", "web01"));
    }

    @Test
    void parse_concatenated_events_without_delimiter() throws IOException {
        final String json = "{\"event\": \"first\"}{\"event\": \"second\"}{\"event\": \"third\"}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(3));
        assertThat(result.get(0), hasEntry("event", "first"));
        assertThat(result.get(1), hasEntry("event", "second"));
        assertThat(result.get(2), hasEntry("event", "third"));
    }

    @Test
    void parse_event_with_nested_object() throws IOException {
        final String json = "{\"event\": {\"method\": \"GET\", \"path\": \"/api\"}, \"host\": \"web01\"}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        @SuppressWarnings("unchecked")
        final Map<String, Object> event = (Map<String, Object>) result.get(0).get("event");
        assertThat(event, hasEntry("method", "GET"));
        assertThat(event, hasEntry("path", "/api"));
    }

    @Test
    void parse_event_with_time_field() throws IOException {
        final String json = "{\"event\": \"msg\", \"time\": 1713196800.123}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        assertThat(result.get(0), hasKey("time"));
    }

    @Test
    void parse_event_with_fields() throws IOException {
        final String json = "{\"event\": \"msg\", \"fields\": {\"env\": \"prod\", \"region\": \"us-east\"}}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        @SuppressWarnings("unchecked")
        final Map<String, Object> fields = (Map<String, Object>) result.get(0).get("fields");
        assertThat(fields, hasEntry("env", "prod"));
        assertThat(fields, hasEntry("region", "us-east"));
    }

    @Test
    void parse_event_with_all_metadata_fields() throws IOException {
        final String json = "{\"event\": \"msg\", \"host\": \"h1\", \"source\": \"s1\", \"sourcetype\": \"st1\", \"index\": \"idx1\"}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        assertThat(result.get(0), hasEntry("host", "h1"));
        assertThat(result.get(0), hasEntry("source", "s1"));
        assertThat(result.get(0), hasEntry("sourcetype", "st1"));
        assertThat(result.get(0), hasEntry("index", "idx1"));
    }

    @Test
    void parse_malformed_json_throws_HecParseException_with_event_number() {
        final String json = "{\"event\": \"msg\"}{\"event\": broken}";
        final HecParseException exception = assertThrows(HecParseException.class,
                () -> parser.parse(json));
        assertThat(exception.getEventNumber(), equalTo(1));
    }

    @Test
    void parse_empty_body_returns_empty_list() throws IOException {
        final List<Map<String, Object>> result = parser.parse("");
        assertThat(result, hasSize(0));
    }

    @Test
    void parse_event_with_numeric_event_value() throws IOException {
        final String json = "{\"event\": 42}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        assertThat(result.get(0), hasEntry("event", 42));
    }

    @Test
    void parse_event_with_array_event_value() throws IOException {
        final String json = "{\"event\": [1, 2, 3]}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(1));
        @SuppressWarnings("unchecked")
        final List<Integer> eventArray = (List<Integer>) result.get(0).get("event");
        assertThat(eventArray, hasSize(3));
    }

    @Test
    void parse_concatenated_events_with_whitespace() throws IOException {
        final String json = "{\"event\": \"first\"}  \n  {\"event\": \"second\"}";
        final List<Map<String, Object>> result = parser.parse(json);

        assertThat(result, hasSize(2));
        assertThat(result.get(0), hasEntry("event", "first"));
        assertThat(result.get(1), hasEntry("event", "second"));
    }
}
