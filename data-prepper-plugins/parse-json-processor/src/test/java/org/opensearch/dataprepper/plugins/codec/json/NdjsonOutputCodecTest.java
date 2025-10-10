/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NdjsonOutputCodecTest {
    @Mock
    private NdjsonOutputConfig config;

    @Mock
    private OutputCodecContext codecContext;

    private EventFactory eventFactory;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    @BeforeEach
    void setUp() {
        eventFactory = TestEventFactory.getTestEventFactory();
    }

    private NdjsonOutputCodec createObjectUnderTest() {
        return new NdjsonOutputCodec(config);
    }

    @Test
    void start_does_not_write_to_OutputStream() throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final OutputStream outputStream = mock(OutputStream.class);

        objectUnderTest.start(outputStream, null, codecContext);

        verifyNoInteractions(outputStream);
    }

    @Test
    void writer_does_not_write_to_OutputStream() throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final OutputStream outputStream = mock(OutputStream.class);

        objectUnderTest.createWriter(outputStream, null, codecContext);

        verifyNoInteractions(outputStream);
    }

    @Test
    void write_single() throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final Map<String, Object> eventMap = generateEventMap();
        objectUnderTest.start(outputStream, null, codecContext);
        objectUnderTest.writeEvent(eventFactory.eventBuilder(EventBuilder.class).withData(eventMap).build(), outputStream);
        objectUnderTest.complete(outputStream);

        final Map<?, ?> serializedMap = OBJECT_MAPPER.readValue(outputStream.toByteArray(), Map.class);

        assertThat(serializedMap, equalTo(eventMap));
    }

    @Test
    void write_single_using_writer() throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final Map<String, Object> eventMap = generateEventMap();
        final OutputCodec.Writer writer = objectUnderTest.createWriter(outputStream, null, codecContext);
        writer.writeEvent(eventFactory.eventBuilder(EventBuilder.class).withData(eventMap).build());
        writer.complete();

        final Map<?, ?> serializedMap = OBJECT_MAPPER.readValue(outputStream.toByteArray(), Map.class);

        assertThat(serializedMap, equalTo(eventMap));
    }

    @Test
    void constructor_with_null_extension_should_not_throw_exception() {
        when(config.getExtension()).thenReturn(null);

        final NdjsonOutputCodec codec = createObjectUnderTest();

        assertThat(codec.getExtension(), equalTo("ndjson")); // Should return default extension
    }

    @Test
    void constructor_with_valid_ndjson_extension_should_not_throw_exception() {
        when(config.getExtension()).thenReturn("ndjson");

        final NdjsonOutputCodec codec = createObjectUnderTest();

        assertThat(codec.getExtension(), equalTo("ndjson"));
    }

    @Test
    void constructor_with_valid_jsonl_extension_should_not_throw_exception() {
        when(config.getExtension()).thenReturn("jsonl");

        final NdjsonOutputCodec codec = createObjectUnderTest();

        assertThat(codec.getExtension(), equalTo("jsonl"));
    }

    @Test
    void constructor_with_uppercase_valid_extension_should_not_throw_exception() {
        when(config.getExtension()).thenReturn("NDJSON");

        final NdjsonOutputCodec codec = createObjectUnderTest();

        assertThat(codec.getExtension(), equalTo("NDJSON"));
    }

    @Test
    void constructor_with_invalid_extension_should_throw_exception() {
        when(config.getExtension()).thenReturn("invalid");

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest(),
                "Invalid extension 'invalid'. Allowed values are: ndjson, jsonl");
    }

    @Test
    void constructor_with_null_config_should_throw_exception() {
        assertThrows(NullPointerException.class, () -> new NdjsonOutputCodec(null),
                "NdjsonOutputConfig cannot be null");
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 100})
    void write_multiple(final int numberOfEvents) throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final List<Map<String, Object>> eventMaps = IntStream.range(0, numberOfEvents)
                .mapToObj(i -> generateEventMap())
                .collect(Collectors.toList());
        objectUnderTest.start(outputStream, null, codecContext);

        eventMaps.stream()
                .map(eventMap -> eventFactory.eventBuilder(EventBuilder.class).withData(eventMap).build())
                .forEach(event -> {
                    try {
                        objectUnderTest.writeEvent(event, outputStream);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        objectUnderTest.complete(outputStream);

        final String jsonLinesCombined = new String(outputStream.toByteArray());

        final String[] jsonLines = jsonLinesCombined.split("\n");

        assertThat(jsonLines.length, equalTo(numberOfEvents));

        for (int i = 0; i < numberOfEvents; i++) {
            final Map<String, Object> eventMap = eventMaps.get(i);
            final String jsonLine = jsonLines[i];
            final Map<?, ?> serializedMap = OBJECT_MAPPER.readValue(jsonLine, Map.class);

            assertThat(serializedMap, equalTo(eventMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 100})
    void write_multiple_using_writer(final int numberOfEvents) throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final List<Map<String, Object>> eventMaps = IntStream.range(0, numberOfEvents)
                .mapToObj(i -> generateEventMap())
                .collect(Collectors.toList());
        final OutputCodec.Writer writer = objectUnderTest.createWriter(outputStream, null, codecContext);

        eventMaps.stream()
                .map(eventMap -> eventFactory.eventBuilder(EventBuilder.class).withData(eventMap).build())
                .forEach(event -> {
                    try {
                        writer.writeEvent(event);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        writer.complete();

        final String jsonLinesCombined = new String(outputStream.toByteArray());

        final String[] jsonLines = jsonLinesCombined.split("\n");

        assertThat(jsonLines.length, equalTo(numberOfEvents));

        for (int i = 0; i < numberOfEvents; i++) {
            final Map<String, Object> eventMap = eventMaps.get(i);
            final String jsonLine = jsonLines[i];
            final Map<?, ?> serializedMap = OBJECT_MAPPER.readValue(jsonLine, Map.class);

            assertThat(serializedMap, equalTo(eventMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void writer_write_with_empty_values_writes_each_empty_event(final int numberOfEvents) throws IOException {
        final NdjsonOutputCodec objectUnderTest = createObjectUnderTest();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodec.Writer writer = objectUnderTest.createWriter(outputStream, null, codecContext);

        IntStream.range(0, numberOfEvents)
                .mapToObj(eventMap -> eventFactory.eventBuilder(EventBuilder.class).withData(Collections.emptyMap()).build())
                .forEach(event -> {
                    try {
                        writer.writeEvent(event);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        objectUnderTest.complete(outputStream);

        final String jsonLinesCombined = new String(outputStream.toByteArray());

        final String[] jsonLines = jsonLinesCombined.split("\n");

        assertThat(jsonLines.length, equalTo(numberOfEvents));

        for (int i = 0; i < numberOfEvents; i++) {
            final String jsonLine = jsonLines[i];
            final Map<?, ?> serializedMap = OBJECT_MAPPER.readValue(jsonLine, Map.class);

            assertThat(serializedMap, notNullValue());
            assertThat(serializedMap, is(anEmptyMap()));
        }
    }

    private static Map<String, Object> generateEventMap() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 1; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return jsonObject;
    }

}