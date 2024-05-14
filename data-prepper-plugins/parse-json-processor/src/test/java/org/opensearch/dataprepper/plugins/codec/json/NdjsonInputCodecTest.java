/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
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
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NdjsonInputCodecTest {
    @Mock
    private NdjsonInputConfig config;

    private EventFactory eventFactory;

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        eventFactory = TestEventFactory.getTestEventFactory();
    }

    private NdjsonInputCodec createObjectUnderTest() {
        return new NdjsonInputCodec(config, eventFactory);
    }

    @Test
    void parse_with_null_InputStream_throws() {
        final NdjsonInputCodec objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse(null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_InputStream_null_Consumer_throws() {
        final NdjsonInputCodec objectUnderTest = createObjectUnderTest();

        final InputStream inputStream = mock(InputStream.class);
        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse(inputStream, null));

        verifyNoInteractions(inputStream);
    }

    @Test
    void parse_with_empty_InputStream_does_not_call_Consumer() throws IOException {
        final ByteArrayInputStream emptyInputStream = new ByteArrayInputStream(new byte[]{});

        createObjectUnderTest().parse(emptyInputStream, eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ArgumentsSource(ValidInputStreamFormatsArgumentsProvider.class)
    void parse_includes_objects_from_single_line_of_JSON_objects(final InputStreamFormat inputStreamFormat, final int numberOfObjects) throws IOException {
        final List<Map<String, Object>> objects = IntStream.range(0, numberOfObjects)
                .mapToObj(i -> generateJson())
                .collect(Collectors.toList());

        final InputStream inputStream = inputStreamFormat.createInputStream(objects);

        createObjectUnderTest().parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> eventConsumerCaptor = ArgumentCaptor.forClass(Record.class);

        verify(eventConsumer, times(numberOfObjects)).accept(eventConsumerCaptor.capture());

        final List<Record<Event>> capturedRecords = eventConsumerCaptor.getAllValues();

        for (int i = 0; i < numberOfObjects; i++) {
            final Map<String, Object> expectedObject = objects.get(i);
            final Record<Event> actualRecord = capturedRecords.get(i);
            assertThat(actualRecord, notNullValue());
            final Event actualEvent = actualRecord.getData();
            assertThat(actualEvent, notNullValue());

            final Map<String, Object> actualEventMap = actualEvent.toMap();
            assertThat(actualEventMap, equalTo(expectedObject));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ValidInputStreamFormatsArgumentsProvider.class)
    void parse_excludes_empty_objects(final InputStreamFormat inputStreamFormat, final int numberOfObjects) throws IOException {
        final List<Map<String, Object>> objects = new ArrayList<>();
        final List<Map<String, Object>> expectedObjects = new ArrayList<>();
        for (int i = 0; i < numberOfObjects; i++) {
            final Map<String, Object> emptyJson = Collections.emptyMap();
            final Map<String, Object> json = generateJson();
            objects.add(emptyJson);
            objects.add(json);
            expectedObjects.add(json);
        }

        final InputStream inputStream = inputStreamFormat.createInputStream(objects);

        createObjectUnderTest().parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> eventConsumerCaptor = ArgumentCaptor.forClass(Record.class);

        verify(eventConsumer, times(numberOfObjects)).accept(eventConsumerCaptor.capture());

        final List<Record<Event>> capturedRecords = eventConsumerCaptor.getAllValues();

        for (int i = 0; i < numberOfObjects; i++) {
            final Map<String, Object> expectedObject = expectedObjects.get(i);
            final Record<Event> actualRecord = capturedRecords.get(i);
            assertThat(actualRecord, notNullValue());
            final Event actualEvent = actualRecord.getData();
            assertThat(actualEvent, notNullValue());

            final Map<String, Object> actualEventMap = actualEvent.toMap();
            assertThat(actualEventMap, equalTo(expectedObject));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ValidInputStreamFormatsArgumentsProvider.class)
    void parse_includes_empty_objects_when_configured(final InputStreamFormat inputStreamFormat, final int numberOfObjects) throws IOException {
        final List<Map<String, Object>> objects = new ArrayList<>();
        for (int i = 0; i < numberOfObjects; i++) {
            final Map<String, Object> emptyJson = Collections.emptyMap();
            final Map<String, Object> json = generateJson();
            objects.add(emptyJson);
            objects.add(json);
        }

        final InputStream inputStream = inputStreamFormat.createInputStream(objects);

        when(config.isIncludeEmptyObjects()).thenReturn(true);
        createObjectUnderTest().parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> eventConsumerCaptor = ArgumentCaptor.forClass(Record.class);

        verify(eventConsumer, times(objects.size())).accept(eventConsumerCaptor.capture());

        final List<Record<Event>> capturedRecords = eventConsumerCaptor.getAllValues();

        for (int i = 0; i < numberOfObjects; i++) {
            final Map<String, Object> expectedObject = objects.get(2*i+1);
            final Record<Event> expectedEmptyRecord = capturedRecords.get(2*i);
            assertThat(expectedEmptyRecord, notNullValue());
            assertThat(expectedEmptyRecord.getData(), notNullValue());
            assertThat(expectedEmptyRecord.getData().toMap().size(), equalTo(0));

            final Record<Event> actualRecord = capturedRecords.get(2*i+1);
            assertThat(actualRecord, notNullValue());
            final Event actualEvent = actualRecord.getData();
            assertThat(actualEvent, notNullValue());

            final Map<String, Object> actualEventMap = actualEvent.toMap();
            assertThat(actualEventMap, equalTo(expectedObject));
        }
    }

    static class ValidInputStreamFormatsArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    arguments(new StrictNdJsonInputStreamFormat(), 1),
                    arguments(new StrictNdJsonInputStreamFormat(), 2),
                    arguments(new StrictNdJsonInputStreamFormat(), 10),
                    arguments(new AllObjectsOnSameLineInputStreamFormat(), 1),
                    arguments(new AllObjectsOnSameLineInputStreamFormat(), 2),
                    arguments(new AllObjectsOnSameLineInputStreamFormat(), 10),
                    arguments(new AllObjectsOnSameLineWithSpacesInputStreamFormat(), 10),
                    arguments(new MixedInputStreamFormat(), 3),
                    arguments(new MixedInputStreamFormat(), 10)
            );
        }
    }

    interface InputStreamFormat {
        InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException;
    }

    static class StrictNdJsonInputStreamFormat implements InputStreamFormat {
        @Override
        public InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException {
            final StringWriter writer = new StringWriter();

            for (final Map<String, Object> jsonObject : jsonObjects) {
                writer.append(OBJECT_MAPPER.writeValueAsString(jsonObject));
                writer.append(System.lineSeparator());
            }

            return new ByteArrayInputStream(writer.toString().getBytes());
        }

        @Override
        public String toString() {
            return "Strict ND-JSON";
        }
    }

    static class AllObjectsOnSameLineInputStreamFormat implements InputStreamFormat {
        @Override
        public InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException {
            final StringWriter writer = new StringWriter();

            for (final Map<String, Object> jsonObject : jsonObjects) {
                writer.append(OBJECT_MAPPER.writeValueAsString(jsonObject));
            }

            return new ByteArrayInputStream(writer.toString().getBytes());
        }

        @Override
        public String toString() {
            return "Single line";
        }
    }

    static class AllObjectsOnSameLineWithSpacesInputStreamFormat implements InputStreamFormat {
        @Override
        public InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException {
            final StringWriter writer = new StringWriter();

            for (final Map<String, Object> jsonObject : jsonObjects) {
                writer.append(OBJECT_MAPPER.writeValueAsString(jsonObject));
                writer.append(" ");
            }

            return new ByteArrayInputStream(writer.toString().getBytes());
        }

        @Override
        public String toString() {
            return "Spaces";
        }
    }

    static class MixedInputStreamFormat implements InputStreamFormat {
        @Override
        public InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException {
            final StringWriter writer = new StringWriter();

            int counter = 0;
            for (final Map<String, Object> jsonObject : jsonObjects) {
                writer.append(OBJECT_MAPPER.writeValueAsString(jsonObject));
                if(counter % 2 == 0)
                    writer.append(System.lineSeparator());
                counter++;
            }

            return new ByteArrayInputStream(writer.toString().getBytes());
        }

        @Override
        public String toString() {
            return "Mixed";
        }
    }

    private static Map<String, Object> generateJson() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 1; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return jsonObject;
    }
}