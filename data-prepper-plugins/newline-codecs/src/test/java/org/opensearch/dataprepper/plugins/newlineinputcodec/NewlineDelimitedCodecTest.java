/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.newlineinputcodec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NewlineDelimitedCodecTest {

    @Mock
    private NewlineDelimitedInputConfig config;

    private NewlineDelimitedInputCodec createObjectUnderTest() {
        return new NewlineDelimitedInputCodec(config);
    }

    @Test
    void constructor_throws_if_config_is_null() {
        config = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_header_destination_is_empty() throws NoSuchFieldException, IllegalAccessException {
        final NewlineDelimitedInputConfig objectUnderTest = new NewlineDelimitedInputConfig();
        reflectivelySetField(objectUnderTest, "headerDestination", "");
        assertThat(objectUnderTest.isValidHeaderDestination(), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, Integer.MIN_VALUE})
    void constructor_throws_if_skipLines_is_less_than_zero(final int negativeSkipLines) {
        when(config.getSkipLines()).thenReturn(negativeSkipLines);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 10, 50})
    void parse_calls_Consumer_for_each_line(final int numberOfLines) throws IOException {
        final List<String> linesList = generateLinesAsList(numberOfLines);
        final InputStream inputStream = createInputStream(linesList);

        final List<Record<Event>> actualEvents = new ArrayList<>();
        createObjectUnderTest().parse(inputStream, actualEvents::add);

        assertThat(actualEvents.size(), equalTo(numberOfLines));
        for (int i = 0; i < actualEvents.size(); i++) {
            final Record<Event> record = actualEvents.get(i);
            assertThat(record, notNullValue());
            assertThat(record.getData(), notNullValue());
            assertThat(record.getData().get("message", String.class), equalTo(linesList.get(i)));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 50})
    void parse_calls_Consumer_for_each_line_after_skipping(final int numberOfLines) throws IOException {
        final List<String> linesList = generateLinesAsList(numberOfLines);
        final InputStream inputStream = createInputStream(linesList);

        final int skipLines = 1;
        when(config.getSkipLines()).thenReturn(skipLines);
        final List<Record<Event>> actualEvents = new ArrayList<>();
        createObjectUnderTest().parse(inputStream, actualEvents::add);

        assertThat(actualEvents.size(), equalTo(numberOfLines - skipLines));
        for (int i = 0; i < actualEvents.size(); i++) {
            final Record<Event> record = actualEvents.get(i);
            assertThat(record, notNullValue());
            assertThat(record.getData(), notNullValue());
            assertThat(record.getData().get("message", String.class), equalTo(linesList.get(i + skipLines)));
            assertThat(record.getData().getMetadata(), notNullValue());
            assertThat(record.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }
    }

    @Test
    void parse_on_empty_InputStream_with_skipLines_does_not_call_Consumer() throws IOException {
        final InputStream inputStream = createInputStream(generateLinesAsList(0));

        when(config.getSkipLines()).thenReturn(1);
        final Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        createObjectUnderTest().parse(inputStream, eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 10, 50})
    void parse_with_header_calls_Consumer_with_header_fields_no_skip(final int numberOfLines) throws IOException {
        final String headerMessage = "HeaderOnList";

        final List<String> linesList = generateLinesAsListWithHeader(numberOfLines, headerMessage);
        final InputStream inputStream = createInputStream(linesList);

        final int headerOffset = 1;
        when(config.getHeaderDestination()).thenReturn("event_header");
        final List<Record<Event>> actualEvents = new ArrayList<>();
        createObjectUnderTest().parse(inputStream, actualEvents::add);

        assertThat(actualEvents.size(), equalTo(numberOfLines));
        for (int i = headerOffset; i < actualEvents.size(); i++) {
            final Record<Event> record = actualEvents.get(i);
            assertThat(record, notNullValue());
            assertThat(record.getData(), notNullValue());
            assertThat(record.getData().get("event_header", String.class), equalTo(headerMessage));
            assertThat(record.getData().get("message", String.class), equalTo(linesList.get(i + headerOffset)));
            assertThat(record.getData().getMetadata(), notNullValue());
            assertThat(record.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 50})
    void parse_with_header_calls_Consumer_with_header_fields_after_skip(final int numberOfLines) throws IOException {
        final String headerMessage = "HeaderOnList";
        final int skipLines = 1;

        final List<String> linesList = generateLinesAsListWithHeaderAfterJunkLines(numberOfLines, headerMessage, skipLines);
        final InputStream inputStream = createInputStream(linesList);

        final int headerOffset = 1;
        when(config.getHeaderDestination()).thenReturn("event_header");
        when(config.getSkipLines()).thenReturn(skipLines);
        final List<Record<Event>> actualEvents = new ArrayList<>();
        createObjectUnderTest().parse(inputStream, actualEvents::add);

        assertThat(actualEvents.size(), equalTo(numberOfLines));
        for (int i = headerOffset; i < actualEvents.size(); i++) {
            final Record<Event> record = actualEvents.get(i);
            assertThat(record, notNullValue());
            assertThat(record.getData(), notNullValue());
            assertThat(record.getData().get("event_header", String.class), equalTo(headerMessage));
            assertThat(record.getData().get("message", String.class), equalTo(linesList.get(i + skipLines + headerOffset)));
            assertThat(record.getData().getMetadata(), notNullValue());
            assertThat(record.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 50})
    void parse_with_header_calls_Consumer_with_header_fields_after_multiple_skips(final int numberOfLines) throws IOException {
        final String headerMessage = "HeaderOnList";
        final int skipLines = 3;

        final List<String> linesList = generateLinesAsListWithHeaderAfterJunkLines(numberOfLines, headerMessage, skipLines);
        final InputStream inputStream = createInputStream(linesList);

        final int headerOffset = 1;
        when(config.getHeaderDestination()).thenReturn("event_header");
        when(config.getSkipLines()).thenReturn(skipLines);
        final List<Record<Event>> actualEvents = new ArrayList<>();
        createObjectUnderTest().parse(inputStream, actualEvents::add);

        assertThat(actualEvents.size(), equalTo(numberOfLines));
        for (int i = headerOffset; i < actualEvents.size(); i++) {
            final Record<Event> record = actualEvents.get(i);
            assertThat(record, notNullValue());
            assertThat(record.getData(), notNullValue());
            assertThat(record.getData().get("event_header", String.class), equalTo(headerMessage));
            assertThat(record.getData().get("message", String.class), equalTo(linesList.get(i + skipLines + headerOffset)));
            assertThat(record.getData().getMetadata(), notNullValue());
            assertThat(record.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }
    }

    private List<String> generateLinesAsListWithHeaderAfterJunkLines(int numberOfLines, String headerMessage, int numJunkLines) {
        final int headerOffset = 1;
//        final int SKIP_OFFSET = 1;
        final List<String> linesList = new ArrayList<>(numberOfLines+headerOffset+numJunkLines);
        for (int i = 0; i < numJunkLines; i++) {
            linesList.add("JUNK VALUE TO BE SKIPPED, Line: " + i);
        }
        linesList.add(headerMessage);
        for (int i = 0; i < numberOfLines; i++)
            linesList.add(UUID.randomUUID().toString());
        return Collections.unmodifiableList(linesList);
    }

    private List<String> generateLinesAsListWithHeader(final int numberOfLines, final String headerMessage) {
        final int headerOffset = 1;
        final List<String> linesList = new ArrayList<>(numberOfLines+headerOffset);
        linesList.add(headerMessage);
        for (int i = 0; i < numberOfLines; i++)
            linesList.add(UUID.randomUUID().toString());
        return Collections.unmodifiableList(linesList);
    }

    private InputStream createInputStream(final List<String> lines) {
        final String inputString = generateMultilineString(lines);

        return new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
    }

    private List<String> generateLinesAsList(final int numberOfLines) {
        final List<String> linesList = new ArrayList<>(numberOfLines);
        for (int i = 0; i < numberOfLines; i++)
            linesList.add(UUID.randomUUID().toString());
        return Collections.unmodifiableList(linesList);
    }

    private String generateMultilineString(final List<String> numberOfLines) {
        final StringWriter stringWriter = new StringWriter();
        for (String line : numberOfLines) {
            stringWriter.write(line);
            stringWriter.write(System.lineSeparator());
        }

        return stringWriter.toString();
    }

    private void reflectivelySetField(final NewlineDelimitedInputConfig newlineDelimitedInputConfig, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = NewlineDelimitedInputConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(newlineDelimitedInputConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }
}