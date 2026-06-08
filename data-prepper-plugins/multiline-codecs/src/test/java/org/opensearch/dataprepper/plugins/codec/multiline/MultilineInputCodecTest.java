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
    void constructor_throws_if_match_pattern_is_invalid() {
        when(config.getMatch()).thenReturn("[invalid(");

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @Test
    void parse_throws_if_inputStream_is_null() {
        when(config.getMatch()).thenReturn("^\\S");
        when(config.getNegate()).thenReturn(true);
        when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");

        final MultilineInputCodec codec = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> codec.parse(null, events -> {}));
    }

    @Test
    void parse_throws_if_consumer_is_null() {
        when(config.getMatch()).thenReturn("^\\S");
        when(config.getNegate()).thenReturn(true);
        when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");

        final MultilineInputCodec codec = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> codec.parse(toInputStream("test"), null));
    }

    @Test
    void parse_empty_input_produces_no_events() throws IOException {
        when(config.getMatch()).thenReturn("^\\S");
        when(config.getNegate()).thenReturn(true);
        when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");

        final List<Record<Event>> events = parseContent("");
        assertThat(events.size(), equalTo(0));
    }

    @Test
    void parse_single_line_produces_one_event() throws IOException {
        when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}");
        when(config.getNegate()).thenReturn(true);
        when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");

        final List<Record<Event>> events = parseContent("2024-01-01 INFO single line\n");
        assertThat(events.size(), equalTo(1));
        assertThat(events.get(0).getData().get("message", String.class), equalTo("2024-01-01 INFO single line"));
    }

    @Nested
    class PreviousModeWithNegateTrue {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void groups_java_stack_trace_with_timestamp_start() throws IOException {
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
        void multiple_single_line_events_each_matching_pattern() throws IOException {
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
        void continuation_lines_at_beginning_are_grouped_as_first_event() throws IOException {
            final String input = "  orphan continuation line 1\n" +
                    "  orphan continuation line 2\n" +
                    "2024-01-01 INFO first real entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  orphan continuation line 1\n  orphan continuation line 2"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("2024-01-01 INFO first real entry"));
        }

        @Test
        void last_event_with_continuations_flushed_at_end_of_stream() throws IOException {
            final String input = "2024-01-01 ERROR Exception occurred\n" +
                    "  at com.example.Foo.bar(Foo.java:1)\n" +
                    "  at com.example.Baz.run(Baz.java:2)\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR Exception occurred\n" +
                            "  at com.example.Foo.bar(Foo.java:1)\n" +
                            "  at com.example.Baz.run(Baz.java:2)"));
        }

        @Test
        void no_lines_match_pattern_produces_single_event() throws IOException {
            final String input = "  continuation line 1\n" +
                    "  continuation line 2\n" +
                    "  continuation line 3\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  continuation line 1\n  continuation line 2\n  continuation line 3"));
        }
    }

    @Nested
    class PreviousModeWithNegateFalse {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\s+(at |\\.\\.\\.|Caused by:)");
            when(config.getNegate()).thenReturn(false);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void groups_stack_trace_lines_matching_pattern_with_previous() throws IOException {
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
        void caused_by_is_grouped_with_previous() throws IOException {
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
    class NextMode {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\s");
            when(config.getNegate()).thenReturn(false);
            when(config.getWhat()).thenReturn(MultilineWhat.NEXT);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
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
        void multiple_groups_in_next_mode() throws IOException {
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
        void trailing_continuation_lines_flushed_at_end_of_stream() throws IOException {
            final String input = "EVENT A\n" +
                    "  trailing context 1\n" +
                    "  trailing context 2\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("EVENT A"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  trailing context 1\n  trailing context 2"));
        }

        @Test
        void no_continuation_lines_each_line_is_separate_event() throws IOException {
            final String input = "EVENT A\n" +
                    "EVENT B\n" +
                    "EVENT C\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class), equalTo("EVENT A"));
            assertThat(events.get(1).getData().get("message", String.class), equalTo("EVENT B"));
            assertThat(events.get(2).getData().get("message", String.class), equalTo("EVENT C"));
        }
    }

    @Nested
    class NextModeMaxLinesLimit {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\d{4}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.NEXT);
            when(config.getMaxLines()).thenReturn(3);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void flushes_continuation_lines_when_max_lines_exceeded_in_next_mode() throws IOException {
            final String input = "  ctx 1\n" +
                    "  ctx 2\n" +
                    "  ctx 3\n" +
                    "  ctx 4\n" +
                    "2024 EVENT\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("  ctx 1\n  ctx 2\n  ctx 3"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  ctx 4\n2024 EVENT"));
        }
    }

    @Nested
    class NextModeWithNegateTrue {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\[");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.NEXT);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void lines_not_matching_pattern_are_prepended_to_next_matching_line() throws IOException {
            final String input = "preamble line 1\n" +
                    "preamble line 2\n" +
                    "[2024-01-01] Log entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(1));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("preamble line 1\npreamble line 2\n[2024-01-01] Log entry"));
        }
    }

    @Nested
    class MaxLinesLimit {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\d{4}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(3);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void flushes_event_when_max_lines_exceeded() throws IOException {
            final String input = "2024-01-01 ERROR start\n" +
                    "  line 2\n" +
                    "  line 3\n" +
                    "  line 4\n" +
                    "  line 5\n" +
                    "2024-01-02 INFO next\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR start\n  line 2\n  line 3"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  line 4\n  line 5"));
            assertThat(events.get(2).getData().get("message", String.class),
                    equalTo("2024-01-02 INFO next"));
        }
    }

    @Nested
    class MaxLengthLimit {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\d{4}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(30);
            when(config.getLineSeparator()).thenReturn("\n");
        }

        @Test
        void flushes_event_when_max_length_exceeded() throws IOException {
            final String input = "2024 start line here\n" +
                    "  continuation is long\n" +
                    "2024 next entry\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(3));
            // First event is "2024 start line here" (20 chars)
            // Adding "\n  continuation is long" would be 20+1+22=43 > 30, so it flushes
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024 start line here"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("  continuation is long"));
            assertThat(events.get(2).getData().get("message", String.class),
                    equalTo("2024 next entry"));
        }
    }

    @Nested
    class CustomLineSeparator {

        @BeforeEach
        void setUp() {
            when(config.getMatch()).thenReturn("^\\d{4}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\r\n");
        }

        @Test
        void uses_custom_line_separator_when_joining() throws IOException {
            final String input = "2024-01-01 ERROR start\n" +
                    "  continuation\n" +
                    "2024-01-02 INFO next\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 ERROR start\r\n  continuation"));
        }
    }

    @Nested
    class RealWorldScenarios {

        @Test
        void python_traceback() throws IOException {
            when(config.getMatch()).thenReturn("^Traceback|^\\s|^\\w+Error");
            when(config.getNegate()).thenReturn(false);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");

            final String input = "2024-01-01 INFO Starting application\n" +
                    "Traceback (most recent call last):\n" +
                    "  File \"main.py\", line 10, in <module>\n" +
                    "    result = process()\n" +
                    "  File \"service.py\", line 5, in process\n" +
                    "    return 1/0\n" +
                    "ZeroDivisionError: division by zero\n" +
                    "2024-01-01 INFO Recovered\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 INFO Starting application\n" +
                            "Traceback (most recent call last):\n" +
                            "  File \"main.py\", line 10, in <module>\n" +
                            "    result = process()\n" +
                            "  File \"service.py\", line 5, in process\n" +
                            "    return 1/0\n" +
                            "ZeroDivisionError: division by zero"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("2024-01-01 INFO Recovered"));
        }

        @Test
        void multiline_xml_in_logs() throws IOException {
            when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");

            final String input = "2024-01-01 Request body:\n" +
                    "<root>\n" +
                    "  <element>value</element>\n" +
                    "</root>\n" +
                    "2024-01-01 Response sent\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01 Request body:\n<root>\n  <element>value</element>\n</root>"));
            assertThat(events.get(1).getData().get("message", String.class),
                    equalTo("2024-01-01 Response sent"));
        }

        @Test
        void log4j_multiline_with_nested_exception() throws IOException {
            when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");

            final String input = "2024-01-01T12:00:00 ERROR Application failed\n" +
                    "java.lang.RuntimeException: Outer\n" +
                    "\tat com.example.A.run(A.java:10)\n" +
                    "Caused by: java.io.IOException: Inner\n" +
                    "\tat com.example.B.read(B.java:20)\n" +
                    "\t... 5 more\n" +
                    "2024-01-01T12:00:01 INFO Shutdown complete\n";

            final List<Record<Event>> events = parseContent(input);

            assertThat(events.size(), equalTo(2));
            assertThat(events.get(0).getData().get("message", String.class),
                    equalTo("2024-01-01T12:00:00 ERROR Application failed\n" +
                            "java.lang.RuntimeException: Outer\n" +
                            "\tat com.example.A.run(A.java:10)\n" +
                            "Caused by: java.io.IOException: Inner\n" +
                            "\tat com.example.B.read(B.java:20)\n" +
                            "\t... 5 more"));
        }
    }

    @Nested
    class IsContinuationLineTests {

        @Test
        void negate_false_matching_line_is_continuation() {
            when(config.getMatch()).thenReturn("^\\s");
            when(config.getNegate()).thenReturn(false);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");

            final MultilineInputCodec codec = createObjectUnderTest();
            assertThat(codec.isContinuationLine("  indented"), equalTo(true));
            assertThat(codec.isContinuationLine("not indented"), equalTo(false));
        }

        @Test
        void negate_true_non_matching_line_is_continuation() {
            when(config.getMatch()).thenReturn("^\\d{4}");
            when(config.getNegate()).thenReturn(true);
            when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
            when(config.getMaxLines()).thenReturn(500);
            when(config.getMaxLength()).thenReturn(10000);
            when(config.getLineSeparator()).thenReturn("\n");

            final MultilineInputCodec codec = createObjectUnderTest();
            assertThat(codec.isContinuationLine("  no timestamp"), equalTo(true));
            assertThat(codec.isContinuationLine("2024 has timestamp"), equalTo(false));
        }
    }

    @Test
    void event_metadata_is_log_type() throws IOException {
        when(config.getMatch()).thenReturn("^\\d{4}");
        when(config.getNegate()).thenReturn(true);
        when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        when(config.getMaxLines()).thenReturn(500);
        when(config.getMaxLength()).thenReturn(10000);
        when(config.getLineSeparator()).thenReturn("\n");

        final List<Record<Event>> events = parseContent("2024-01-01 test\n");

        assertThat(events.size(), equalTo(1));
        assertThat(events.get(0).getData(), notNullValue());
        assertThat(events.get(0).getData().getMetadata(), notNullValue());
        assertThat(events.get(0).getData().getMetadata().getEventType(), equalTo("LOG"));
    }
}
