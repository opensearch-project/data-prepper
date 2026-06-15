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
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
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
        lenient().when(config.getOmitMatchedSection()).thenReturn(false);
        lenient().when(config.getEncoding()).thenReturn(StandardCharsets.UTF_8);
    }

    private MultilineInputCodec createObjectUnderTest() {
        return new MultilineInputCodec(config, eventFactory);
    }

    private InputStream toInputStream(final String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void parse_java_stack_trace_with_event_start_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}"));
        lenient().when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");

        final String input =
                "2024-01-15 10:23:45.123 ERROR [main] com.example.UserService - Request failed\n" +
                "java.lang.NullPointerException: null\n" +
                "\tat com.example.UserService.getUser(UserService.java:42)\n" +
                "\tat com.example.Controller.handle(Controller.java:28)\n" +
                "Caused by: java.sql.SQLException: Connection refused\n" +
                "\tat com.mysql.jdbc.Connection.connect(Connection.java:456)\n" +
                "2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("NullPointerException"));
        assertThat(event1, containsString("at com.example.UserService.getUser"));
        assertThat(event1, containsString("Caused by: java.sql.SQLException"));
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("2024-01-15 10:23:45.456 INFO [main] com.example.UserService - Retrying"));
    }

    @Test
    void parse_python_traceback_with_event_start_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}"));
        lenient().when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");

        final String input =
                "2024-03-20 08:15:00,123 INFO Starting application\n" +
                "2024-03-20 08:15:02,789 ERROR Unhandled exception\n" +
                "Traceback (most recent call last):\n" +
                "  File \"/app/worker.py\", line 45, in process\n" +
                "ValueError: invalid literal for int()\n" +
                "2024-03-20 08:15:03,456 INFO Recovered\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        assertThat(records.get(0).getData().get("message", String.class),
                equalTo("2024-03-20 08:15:00,123 INFO Starting application"));
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("Traceback"));
        assertThat(event2, containsString("ValueError"));
        assertThat(records.get(2).getData().get("message", String.class),
                equalTo("2024-03-20 08:15:03,456 INFO Recovered"));
    }

    @Test
    void parse_xml_multiline_with_event_start_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\[\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}"));
        lenient().when(config.getEventStartPattern()).thenReturn("^\\[\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}");

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
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("<root>"));
        assertThat(event1, containsString("</root>"));
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("[2024-05-10 14:30:00.045] [INFO] Request processed"));
    }

    @Test
    void parse_syslog_ise_with_event_start_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^<\\d+>[A-Z][a-z]{2}\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}"));
        lenient().when(config.getEventStartPattern()).thenReturn("^<\\d+>[A-Z][a-z]{2}\\s+\\d+\\s+\\d{2}:\\d{2}:\\d{2}");

        final String input =
                "<181>Jun  1 12:39:49 Infra-ISE Audit NOTICE Admin-Login: success\n" +
                "<181>Jun  1 12:39:49 Infra-ISE Audit NOTICE OpenAPI: Response={\n" +
                "  \"version\" : \"1.0.0\"\n" +
                "}, HttpCode=200\n" +
                "<181>Jun  1 12:40:15 Infra-ISE Audit NOTICE Config-Change: added\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(3)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        assertThat(records.get(0).getData().get("message", String.class),
                containsString("Admin-Login: success"));
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("OpenAPI: Response="));
        assertThat(event2, containsString("\"version\" : \"1.0.0\""));
        assertThat(event2, containsString("HttpCode=200"));
        assertThat(records.get(2).getData().get("message", String.class),
                containsString("Config-Change: added"));
    }

    @Test
    void parse_with_continuation_line_start_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\s+(at |\\.\\.\\.|Caused by:)"));
        lenient().when(config.getContinuationLineStartPattern()).thenReturn("^\\s+(at |\\.\\.\\.|Caused by:)");

        final String input =
                "java.lang.RuntimeException: error\n" +
                "  at com.example.A.method(A.java:1)\n" +
                "  Caused by: java.io.IOException\n" +
                "  at com.example.C.read(C.java:3)\n" +
                "Application recovered\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("RuntimeException: error"));
        assertThat(event1, containsString("at com.example.A.method"));
        assertThat(event1, containsString("Caused by: java.io.IOException"));
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("Application recovered"));
    }

    @Test
    void parse_with_event_end_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^---$"));
        lenient().when(config.getEventEndPattern()).thenReturn("^---$");

        final String input =
                "entry 1 line 1\n" +
                "entry 1 line 2\n" +
                "---\n" +
                "entry 2 line 1\n" +
                "---\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        assertThat(records.get(0).getData().get("message", String.class),
                equalTo("entry 1 line 1\nentry 1 line 2\n---"));
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("entry 2 line 1\n---"));
    }

    @Test
    void parse_with_continuation_end_pattern() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\s"));
        lenient().when(config.getContinuationLineEndPattern()).thenReturn("^\\s");

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
        final String event1 = records.get(0).getData().get("message", String.class);
        assertThat(event1, containsString("context-line-1"));
        assertThat(event1, containsString("MAIN EVENT A"));
        final String event2 = records.get(1).getData().get("message", String.class);
        assertThat(event2, containsString("context-line-3"));
        assertThat(event2, containsString("MAIN EVENT B"));
    }

    @Test
    void parse_with_omit_matched_section() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}-\\d{2}-\\d{2}\\s+"));
        lenient().when(config.getEventStartPattern()).thenReturn("^\\d{4}-\\d{2}-\\d{2}\\s+");
        lenient().when(config.getOmitMatchedSection()).thenReturn(true);

        final String input =
                "2024-01-01 ERROR something bad\n" +
                "  stack trace\n" +
                "2024-01-02 INFO recovered\n";

        createObjectUnderTest().parse(toInputStream(input), eventConsumer);

        final ArgumentCaptor<Record<Event>> captor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(2)).accept(captor.capture());

        final List<Record<Event>> records = captor.getAllValues();
        assertThat(records.get(0).getData().get("message", String.class),
                equalTo("ERROR something bad\n  stack trace"));
        assertThat(records.get(1).getData().get("message", String.class),
                equalTo("INFO recovered"));
    }

    @Test
    void parse_empty_input_produces_no_events() throws IOException {
        lenient().when(config.getCompiledPattern()).thenReturn(Pattern.compile("^\\d{4}"));
        lenient().when(config.getEventStartPattern()).thenReturn("^\\d{4}");

        createObjectUnderTest().parse(toInputStream(""), eventConsumer);

        verify(eventConsumer, times(0)).accept(ArgumentCaptor.forClass(Record.class).capture());
    }
}
