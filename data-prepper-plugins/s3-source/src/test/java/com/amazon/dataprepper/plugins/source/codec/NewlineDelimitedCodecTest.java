/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NewlineDelimitedCodecTest {

    @Mock
    private NewlineDelimitedConfig config;

    private NewlineDelimitedCodec createObjectUnderTest() {
        return new NewlineDelimitedCodec(config);
    }

    @Test
    void constructor_throws_if_config_is_null() {
        config = null;

        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -2, Integer.MIN_VALUE})
    void constructor_throws_if_skipLines_is_less_than_zero(int negativeSkipLines) {
        when(config.getSkipLines()).thenReturn(negativeSkipLines);

        assertThrows(IllegalArgumentException.class, this::createObjectUnderTest);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 10, 50})
    void parse_calls_Consumer_for_each_line(int numberOfLines) throws IOException {
        final List<String> linesList = generateLinesAsList(numberOfLines);
        final InputStream inputStream = createInputStream(linesList);

        List<Record<Event>> actualEvents = new ArrayList<>();
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
    void parse_calls_Consumer_for_each_line_after_skipping(int numberOfLines) throws IOException {
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
        }
    }

    @Test
    void parse_on_empty_InputStream_with_skipLines_does_not_call_Consumer() throws IOException {
        final InputStream inputStream = createInputStream(generateLinesAsList(0));

        when(config.getSkipLines()).thenReturn(1);
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        createObjectUnderTest().parse(inputStream, eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    private InputStream createInputStream(List<String> lines) {
        final String inputString = generateMultilineString(lines);

        return new ByteArrayInputStream(inputString.getBytes(StandardCharsets.UTF_8));
    }

    private List<String> generateLinesAsList(int numberOfLines) {
        List<String> linesList = new ArrayList<>(numberOfLines);
        for (int i = 0; i < numberOfLines; i++)
            linesList.add(UUID.randomUUID().toString());
        return Collections.unmodifiableList(linesList);
    }

    private String generateMultilineString(List<String> numberOfLines) {
        final StringWriter stringWriter = new StringWriter();
        for (String line : numberOfLines) {
            stringWriter.write(line);
            stringWriter.write(System.lineSeparator());
        }

        return stringWriter.toString();
    }
}