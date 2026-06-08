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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class MultilineCodecsIT {

    @Mock
    private MultilineInputCodecConfig config;

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private final EventFactory eventFactory = TestEventFactory.getTestEventFactory();

    @BeforeEach
    void setUp() {
        lenient().when(config.getMaxLines()).thenReturn(500);
        lenient().when(config.getMaxLength()).thenReturn(50000);
        lenient().when(config.getLineSeparator()).thenReturn("\n");
    }

    private MultilineInputCodec createObjectUnderTest() {
        return new MultilineInputCodec(config, eventFactory);
    }

    private InputStream toInputStream(final String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parse_java_stack_trace_groups_exception_with_stack_frames() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "2024-01-15 10:23:45.123 ERROR [main] com.example.UserService - Request failed\n" +
                "java.lang.NullPointerException: null\n" +
                "\tat com.example.UserService.getUser(UserService.java:42)\n" +
                "\tat com.example.Controller.handle(Controller.java:28)\n" +
                "Caused by: java.sql.SQLException: Connection refused\n" +
                "\tat com.mysql.jdbc.Connection.connect(Connection.java:456)\n" +
                "\t... 12 more\n" +
                "2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying\n" +
                "2024-01-15 10:23:46.789 WARN [worker-1] com.example.Cache - Cache miss\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: ERROR log + stack trace (7 lines grouped)
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, notNullValue());
        assertThat(event1, containsString("NullPointerException"));
        assertThat(event1, containsString("at com.example.UserService.getUser"));
        assertThat(event1, containsString("Caused by: java.sql.SQLException"));
        assertThat(event1, containsString("... 12 more"));

        // Event 2: INFO single line
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, equalTo("2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying"));

        // Event 3: WARN single line
        final String event3 = records.get(2).getData().get("message", String.class);
        assertThat(event3, equalTo("2024-01-15 10:23:46.789 WARN [worker-1] com.example.Cache - Cache miss"));
    }

    @Test
    void parse_python_traceback_groups_traceback_with_error_line() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "2024-03-20 08:15:00,123 INFO Starting application\n" +
                "2024-03-20 08:15:02,789 ERROR Unhandled exception\n" +
                "Traceback (most recent call last):\n" +
                "  File \"/app/worker.py\", line 45, in process\n" +
                "    result = transform(record)\n" +
                "ValueError: invalid literal for int()\n" +
                "2024-03-20 08:15:03,456 INFO Recovered\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: single INFO line
        assertThat(records.get(0).getData().get("message", String.class),
                equalTo("2024-03-20 08:15:00,123 INFO Starting application"));

        // Event 2: ERROR + traceback (5 lines grouped)
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("ERROR Unhandled exception"));
        assertThat(event2, containsString("Traceback (most recent call last):"));
        assertThat(event2, containsString("File \"/app/worker.py\""));
        assertThat(event2, containsString("ValueError: invalid literal"));

        // Event 3: single INFO line
        assertThat(records.get(2).getData().get("message", String.class),
                equalTo("2024-03-20 08:15:03,456 INFO Recovered"));
    }

    @Test
    void parse_xml_multiline_logs_groups_xml_body_with_header() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\[\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "[2024-05-10 14:30:00.001] [INFO] Incoming request:\n" +
                "<root>\n" +
                "  <element>value</element>\n" +
                "</root>\n" +
                "[2024-05-10 14:30:00.045] [INFO] Request processed\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: log line + XML body (4 lines grouped)
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("[INFO] Incoming request:"));
        assertThat(event1, containsString("<root>"));
        assertThat(event1, containsString("<element>value</element>"));
        assertThat(event1, containsString("</root>"));

        // Event 2: single line
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("[2024-05-10 14:30:00.045] [INFO] Request processed"));
    }

    @Test
    void parse_sql_multiline_logs_groups_query_with_header() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "2024-07-01 09:00:01 [Query] thread_id=145 exec_time=0.003s\n" +
                "SELECT u.id, u.name\n" +
                "FROM users u\n" +
                "WHERE u.active = 1\n" +
                "ORDER BY u.name;\n" +
                "2024-07-01 09:00:02 [Query] thread_id=146 exec_time=0.001s\n" +
                "SELECT COUNT(*) FROM sessions;\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: query header + multi-line SQL (5 lines grouped)
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("[Query] thread_id=145"));
        assertThat(event1, containsString("SELECT u.id, u.name"));
        assertThat(event1, containsString("FROM users u"));
        assertThat(event1, containsString("WHERE u.active = 1"));
        assertThat(event1, containsString("ORDER BY u.name;"));

        // Event 2: query header + single-line SQL (2 lines grouped)
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("[Query] thread_id=146"));
        assertThat(event2, containsString("SELECT COUNT(*) FROM sessions;"));
    }

    @Test
    void parse_syslog_ise_multiline_groups_continuation_lines() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^<\\d+>[A-Z][a-z]{2}\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "<181>Jun  1 12:39:49 Infra-ISE CISE_Audit 0000000176 NOTICE Admin-Login: success\n" +
                "<181>Jun  1 12:39:49 Infra-ISE CISE_Audit 0000000177 NOTICE OpenAPI: Response={\\\n" +
                "  \"version\" : \"1.0.0\",\\\n" +
                "  \"status\" : \"ok\"\\\n" +
                "}, HttpCode=200\n" +
                "<181>Jun  1 12:40:15 Infra-ISE CISE_Audit 0000000178 NOTICE Config-Change: added\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: single-line syslog
        assertThat(records.get(0).getData().get("message", String.class),
                containsString("Admin-Login: success"));

        // Event 2: multiline syslog with JSON continuation (4 lines grouped)
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("OpenAPI: Response="));
        assertThat(event2, containsString("\"version\" : \"1.0.0\""));
        assertThat(event2, containsString("HttpCode=200"));

        // Event 3: single-line syslog
        assertThat(records.get(2).getData().get("message", String.class),
                containsString("Config-Change: added"));
    }

    @Test
    void parse_with_negate_false_groups_matching_lines_with_previous() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\s+(at |\\.\\.\\.|Caused by:)");
        lenient().when(config.getNegate()).thenReturn(false);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "java.lang.RuntimeException: error\n" +
                "  at com.example.A.method(A.java:1)\n" +
                "  at com.example.B.method(B.java:2)\n" +
                "  Caused by: java.io.IOException\n" +
                "  at com.example.C.read(C.java:3)\n" +
                "  ... 5 more\n" +
                "Application recovered\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: exception + all matching stack frames (6 lines grouped)
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("RuntimeException: error"));
        assertThat(event1, containsString("at com.example.A.method"));
        assertThat(event1, containsString("Caused by: java.io.IOException"));
        assertThat(event1, containsString("... 5 more"));

        // Event 2: non-matching line on its own
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("Application recovered"));
    }

    @Test
    void parse_with_next_mode_prepends_continuation_to_following_event() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\s");
        lenient().when(config.getNegate()).thenReturn(false);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.NEXT);

        final String input =
                "  context-line-1\n" +
                "  context-line-2\n" +
                "MAIN EVENT A\n" +
                "  context-line-3\n" +
                "MAIN EVENT B\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: continuation lines + first non-continuation
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("context-line-1"));
        assertThat(event1, containsString("context-line-2"));
        assertThat(event1, containsString("MAIN EVENT A"));

        // Event 2: continuation line + second non-continuation
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("context-line-3"));
        assertThat(event2, containsString("MAIN EVENT B"));
    }

    @Test
    void parse_respects_max_lines_limit() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        lenient().when(config.getMaxLines()).thenReturn(3);

        final String input =
                "2024 start\n" +
                "  line 2\n" +
                "  line 3\n" +
                "  line 4\n" +
                "  line 5\n" +
                "2024 next event\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: first 3 lines (hit max_lines)
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, equalTo("2024 start\n  line 2\n  line 3"));

        // Event 2: overflow lines
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, equalTo("  line 4\n  line 5"));

        // Event 3: next event
        assertThat(records.get(2).getData().get("message", String.class),
                equalTo("2024 next event"));
    }

    @Test
    void parse_respects_max_length_limit() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);
        lenient().when(config.getMaxLength()).thenReturn(25);

        final String input =
                "2024 start here\n" +
                "  long continuation line\n" +
                "2024 next\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();

        // Event 1: flushed due to max_length before adding continuation
        assertThat(records.get(0).getData().get("message", String.class),
                equalTo("2024 start here"));

        // Event 2: continuation line on its own
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("  long continuation line"));

        // Event 3: next event
        assertThat(records.get(2).getData().get("message", String.class),
                equalTo("2024 next"));
    }

    @Test
    void parse_empty_input_produces_no_events() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        createObjectUnderTest().parse(toInputStream(""), eventConsumer);

        verify(eventConsumer, times(0)).accept(ArgumentCaptor.forClass(Record.class).capture());
    }

    @Test
    void parse_all_lines_are_single_events_when_all_match_pattern() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^\\d{4}");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "2024 event one\n" +
                "2024 event two\n" +
                "2024 event three\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        assertThat(records.get(0).getData().get("message", String.class), equalTo("2024 event one"));
        assertThat(records.get(1).getData().get("message", String.class), equalTo("2024 event two"));
        assertThat(records.get(2).getData().get("message", String.class), equalTo("2024 event three"));
    }

    @Test
    void parse_all_lines_form_single_event_when_none_match_pattern() throws IOException {
        lenient().when(config.getMatch()).thenReturn("^NEVER_MATCHES");
        lenient().when(config.getNegate()).thenReturn(true);
        lenient().when(config.getWhat()).thenReturn(MultilineWhat.PREVIOUS);

        final String input =
                "line one\n" +
                "line two\n" +
                "line three\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(1)).accept(captor.capture());

        assertThat(captor.getValue().getData().get("message", String.class),
                equalTo("line one\nline two\nline three"));
    }
}
