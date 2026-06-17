/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.plugins.DataPrepperPluginTest;
import org.opensearch.dataprepper.test.plugins.PluginConfigurationFile;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

@DataPrepperPluginTest(pluginName = "multiline", pluginType = InputCodec.class)
public class MultilineCodecsIT extends BaseDataPrepperPluginStandardTestSuite {

    private List<Record<Event>> parseContent(final InputCodec codec, final String content) throws IOException {
        final List<Record<Event>> events = new ArrayList<>();
        codec.parse(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), events::add);
        return events;
    }

    @Test
    void parse_java_stack_trace_with_event_start_pattern(
            @PluginConfigurationFile("event-start-pattern.yaml") final InputCodec codec) throws IOException {

        final String input =
                "2024-01-15 10:23:45.123 ERROR [main] com.example.UserService - Request failed\n" +
                "java.lang.NullPointerException: null\n" +
                "\tat com.example.UserService.getUser(UserService.java:42)\n" +
                "\tat com.example.Controller.handle(Controller.java:28)\n" +
                "Caused by: java.sql.SQLException: Connection refused\n" +
                "\tat com.mysql.jdbc.Connection.connect(Connection.java:456)\n" +
                "2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying\n";

        final List<Record<Event>> events = parseContent(codec, input);

        assertThat(events.size(), equalTo(2));
        final String event1 = events.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("NullPointerException"));
        assertThat(event1, containsString("at com.example.UserService.getUser"));
        assertThat(event1, containsString("Caused by: java.sql.SQLException"));
        assertThat(events.get(1).getData().get("message", String.class),
                equalTo("2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying"));
    }

    @Test
    void parse_with_event_end_pattern(
            @PluginConfigurationFile("event-end-pattern.yaml") final InputCodec codec) throws IOException {

        final String input =
                "entry 1 line 1\n" +
                "entry 1 line 2\n" +
                "---\n" +
                "entry 2 line 1\n" +
                "---\n";

        final List<Record<Event>> events = parseContent(codec, input);

        assertThat(events.size(), equalTo(2));
        assertThat(events.get(0).getData().get("message", String.class),
                equalTo("entry 1 line 1\nentry 1 line 2\n---"));
        assertThat(events.get(1).getData().get("message", String.class),
                equalTo("entry 2 line 1\n---"));
    }

    @Test
    void parse_with_continuation_line_start_pattern(
            @PluginConfigurationFile("continuation-line-start-pattern.yaml") final InputCodec codec) throws IOException {

        final String input =
                "java.lang.RuntimeException: error\n" +
                "  at com.example.A.method(A.java:1)\n" +
                "  Caused by: java.io.IOException\n" +
                "  at com.example.C.read(C.java:3)\n" +
                "Application recovered\n";

        final List<Record<Event>> events = parseContent(codec, input);

        assertThat(events.size(), equalTo(2));
        final String event1 = events.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("RuntimeException: error"));
        assertThat(event1, containsString("at com.example.A.method"));
        assertThat(event1, containsString("Caused by: java.io.IOException"));
        assertThat(events.get(1).getData().get("message", String.class),
                equalTo("Application recovered"));
    }

    @Test
    void parse_with_omit_matched_section(
            @PluginConfigurationFile("omit-matched-section.yaml") final InputCodec codec) throws IOException {

        final String input =
                "2024-01-01 ERROR something bad\n" +
                "  stack trace\n" +
                "2024-01-02 INFO recovered\n";

        final List<Record<Event>> events = parseContent(codec, input);

        assertThat(events.size(), equalTo(2));
        assertThat(events.get(0).getData().get("message", String.class),
                equalTo("ERROR something bad\n  stack trace"));
        assertThat(events.get(1).getData().get("message", String.class),
                equalTo("INFO recovered"));
    }

    @Test
    void parse_with_continuation_line_end_pattern(
            @PluginConfigurationFile("continuation-line-end-pattern.yaml") final InputCodec codec) throws IOException {

        final String input =
                "  context-line-1\n" +
                "  context-line-2\n" +
                "MAIN EVENT A\n" +
                "  context-line-3\n" +
                "MAIN EVENT B\n";

        final List<Record<Event>> events = parseContent(codec, input);

        assertThat(events.size(), equalTo(2));
        final String event1 = events.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("context-line-1"));
        assertThat(event1, containsString("context-line-2"));
        assertThat(event1, containsString("MAIN EVENT A"));
        final String event2 = events.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("context-line-3"));
        assertThat(event2, containsString("MAIN EVENT B"));
    }
}
