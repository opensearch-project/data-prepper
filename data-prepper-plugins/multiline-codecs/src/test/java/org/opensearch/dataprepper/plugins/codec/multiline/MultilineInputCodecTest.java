/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultilineInputCodecTest {

    @Mock
    private MultilineInputCodecConfig config;

    private final EventFactory eventFactory = TestEventFactory.getTestEventFactory();

    private MultilineInputCodec createObjectUnderTest() {
        return new MultilineInputCodec(config, eventFactory);
    }

    private InputStream toInputStream(final String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private List<Record<Event>> parseContent(final String content) throws IOException {
        final List<Record<Event>> events = new ArrayList<>();
        createObjectUnderTest().parse(toInputStream(content), events::add);
        return events;
    }

    @Test
    void constructor_throws_if_config_is_null() {
        assertThrows(NullPointerException.class, () -> new MultilineInputCodec(null, eventFactory));
    }

    @Test
    void constructor_throws_if_eventFactory_is_null() {
        assertThrows(NullPointerException.class, () -> new MultilineInputCodec(config, null));
    }

    @Test
    void constructor_throws_if_no_pattern_configured() {
        when(config.getCompiledPattern()).thenReturn(null);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_pattern_is_invalid() {
        when(config.getCompiledPattern()).thenReturn(null);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    private void setupConfig(final String patternStr) {
        when(config.getCompiledPattern()).thenReturn(Pattern.compile(patternStr));
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");
        when(config.getOmitMatchedSection()).thenReturn(false);
        when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);
    }

    @Nested
    class EventStartMode {

        @BeforeEach
        void setUp() {
            setupConfig("^\\d{4}-\\d{2}-\\d{2}");
            when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}");
        }

        @Test
        void groups_stack_trace_with_timestamp_start() throws IOException {
            final String input = "2024-01-01 ERROR NullPointerException\n" +
                    "  at com.example.Service.method(Service.java:42)\n" +
                    "  at com.example.Main.run(Main.java:10)\n" +
                    "2024-01-01 INFO Application recovered\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR NullPointerException\n" +
                            "  at com.example.Service.method(Service.java:42)\n" +
                            "  at com.example.Main.run(Main.java:10)"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("2024-01-01 INFO Application recovered"));
        }

        @Test
        void multiple_single_line_events() throws IOException {
            final String input = "2024-01-01 INFO line one\n" +
                    "2024-01-02 INFO line two\n" +
                    "2024-01-03 INFO line three\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class), equalTo("2024-01-01 INFO line one"));
            assertThat(events.get(1).getData().get("message", String.class), equalTo("2024-01-02 INFO line two"));
            assertThat(events.get(2).getData().get("message", String.class), equalTo("2024-01-03 INFO line three"));
        }

        @Test
        void continuation_lines_at_beginning_grouped_as_first_event() throws IOException {
            final String input = "  orphan line 1\n" +
                    "  orphan line 2\n" +
                    "2024-01-01 INFO first entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  orphan line 1\n  orphan line 2"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("2024-01-01 INFO first entry"));
        }

        @Test
        void last_event_flushed_at_end_of_stream() throws IOException {
            final String input = "2024-01-01 ERROR Exception\n" +
                    "  at com.example.Foo.bar(Foo.java:1)\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR Exception\n  at com.example.Foo.bar(Foo.java:1)"));
        }

        @Test
        void empty_input_produces_no_events() throws IOException {
            final List<Record<Event>> events = parseContent("");
            assertThat(events.size(), equalTo(0));
        }

        @Test
        void no_lines_match_produces_single_event() throws IOException {
            final String input = "  line 1\n  line 2\n  line 3\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  line 1\n  line 2\n  line 3"));
        }
    }

    @Nested
    class EventEndMode {

        @BeforeEach
        void setUp() {
            setupConfig("^---$");
            when(config.getEventEndPattern()).thenReturn("^---$");
        }

        @Test
        void groups_lines_until_separator() throws IOException {
            final String input = "line 1\n" +
                    "line 2\n" +
                    "---\n" +
                    "line 3\n" +
                    "line 4\n" +
                    "---\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("line 1\nline 2\n---"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("line 3\nline 4\n---"));
        }

        @Test
        void trailing_lines_without_end_marker_flushed() throws IOException {
            final String input = "line 1\n" +
                    "---\n" +
                    "line 2\n" +
                    "line 3\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("line 1\n---"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("line 2\nline 3"));
        }

        @Test
        void single_line_matching_end_pattern() throws IOException {
            final String input = "---\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class), equalTo("---"));
        }
    }

    @Nested
    class ContinuationStartMode {

        @BeforeEach
        void setUp() {
            setupConfig("^\\s+(at |\\.\\.\\.|Caused by:)");
            when(config.getContinuationLineStartPattern()).thenReturn("^\\s+(at |\\.\\.\\.|Caused by:)");
        }

        @Test
        void groups_stack_trace_lines_with_previous() throws IOException {
            final String input = "java.lang.NullPointerException: null\n" +
                    "  at com.example.Service.process(Service.java:42)\n" +
                    "  at com.example.Main.run(Main.java:10)\n" +
                    "INFO: Recovery complete\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("java.lang.NullPointerException: null\n" +
                            "  at com.example.Service.process(Service.java:42)\n" +
                            "  at com.example.Main.run(Main.java:10)"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("INFO: Recovery complete"));
        }

        @Test
        void caused_by_grouped_with_previous() throws IOException {
            final String input = "java.lang.RuntimeException: error\n" +
                    "  at com.example.A.method(A.java:1)\n" +
                    "  Caused by: java.io.IOException\n" +
                    "  at com.example.B.read(B.java:5)\n" +
                    "Next log entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("java.lang.RuntimeException: error\n" +
                            "  at com.example.A.method(A.java:1)\n" +
                            "  Caused by: java.io.IOException\n" +
                            "  at com.example.B.read(B.java:5)"));
        }
    }

    @Nested
    class ContinuationEndMode {

        @BeforeEach
        void setUp() {
            setupConfig("^\\s");
        }

        @Test
        void continuation_lines_prepended_to_next_event() throws IOException {
            final String input = "  header line 1\n" +
                    "  header line 2\n" +
                    "MAIN LOG ENTRY\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  header line 1\n  header line 2\nMAIN LOG ENTRY"));
        }

        @Test
        void multiple_groups() throws IOException {
            final String input = "  context A\n" +
                    "EVENT A\n" +
                    "  context B\n" +
                    "EVENT B\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  context A\nEVENT A"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  context B\nEVENT B"));
        }

        @Test
        void trailing_continuation_lines_flushed() throws IOException {
            final String input = "EVENT A\n" +
                    "  trailing 1\n" +
                    "  trailing 2\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class), equalTo("EVENT A"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  trailing 1\n  trailing 2"));
        }

        @Test
        void no_continuation_lines_each_is_separate_event() throws IOException {
            final String input = "EVENT A\nEVENT B\nEVENT C\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class), equalTo("EVENT A"));
            assertThat(events.get(1).getData().get("message", String.class), equalTo("EVENT B"));
            assertThat(events.get(2).getData().get("message", String.class), equalTo("EVENT C"));
        }
    }

    @Nested
    class OmitMatchedSection {

        @Test
        void event_start_pattern_omits_matched_section() throws IOException {
            when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}\\s+"));
            when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+");
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
            when(config.getOmitMatchedSection()).thenReturn(true);
            when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);

            final String input = "2024-01-01 ERROR something\n" +
                    "  stack trace line\n" +
                    "2024-01-02 INFO recovered\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("ERROR something\n  stack trace line"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("INFO recovered"));
        }

        @Test
        void event_end_pattern_omits_matched_section() throws IOException {
            when(config.getCompiledPattern()).thenReturn(Pattern.compile("^---$"));
            when(config.getEventEndPattern()).thenReturn("^---$");
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
            when(config.getOmitMatchedSection()).thenReturn(true);
            when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);

            final String input = "line 1\nline 2\n---\nline 3\n---\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("line 1\nline 2\n"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("line 3\n"));
        }

        @Test
        void omit_false_preserves_matched_section() throws IOException {
            when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}\\s+"));
            when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+");
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
            when(config.getOmitMatchedSection()).thenReturn(false);
            when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);

            final String input = "2024-01-01 ERROR something\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR something"));
        }
    }

    @Nested
    class MaxLinesLimit {

        @BeforeEach
        void setUp() {
            when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}"));
            when(config.getEventStartPattern()).thenReturn("^\\d{4}");
            when(config.getMaxLines()).thenReturn(3);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
            when(config.getOmitMatchedSection()).thenReturn(false);
            when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);
        }

        @Test
        void flushes_event_when_max_lines_exceeded() throws IOException {
            final String input = "2024 start\n  line 2\n  line 3\n  line 4\n  line 5\n2024 next\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024 start\n  line 2\n  line 3"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  line 4\n  line 5"));
            assertThat(events.get(2).getData().get("message", String.class),
                    equalTo("2024 next"));
        }
    }

    @Nested
    class MaxLengthLimit {

        @BeforeEach
        void setUp() {
            when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}"));
            when(config.getEventStartPattern()).thenReturn("^\\d{4}");
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(30);
            when(config.getLineSeparator()).thenReturn("\n");
            when(config.getOmitMatchedSection()).thenReturn(false);
            when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);
        }

        @Test
        void flushes_event_when_max_length_exceeded() throws IOException {
            final String input = "2024 start line here\n  continuation is long\n2024 next entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024 start line here"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  continuation is long"));
            assertThat(events.get(2).getData().get("message", String.class),
                    equalTo("2024 next entry"));
        }
    }

    @Test
    void event_metadata_is_log_type() throws IOException {
        setupConfig("^\\d{4}");
        when(config.getEventStartPattern()).thenReturn("^\\d{4}");

        final List<Record<Event>> events = parseContent("2024-01-01 test\n");

        assertThat(events.size(), equalTo(1));
        assertThat(events.get(0).getData(), notNullValue());
        assertThat(events.get(0).getData().getMetadata(), notNullValue());
        assertThat(events.get(0).getData().getMetadata().getEventType(), equalTo("LOG"));
    }
}
