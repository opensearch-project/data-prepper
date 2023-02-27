/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.jsoninputcodec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;

class JsonInputCodecTest {

    private ObjectMapper objectMapper;
    private Consumer<Record<Event>> eventConsumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        eventConsumer = mock(Consumer.class);
    }

    private JsonInputCodec createObjectUnderTest() {
        return new JsonInputCodec();
    }

    @Test
    void parse_with_null_InputStream_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse(null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_null_Consumer_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

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

    @Test
    void parse_with_InputStream_with_empty_object_does_not_call_Consumer() throws IOException {
        final ByteArrayInputStream emptyObjectInputStream = new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8));

        createObjectUnderTest().parse(emptyObjectInputStream, eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_InputStream_with_object_and_no_array_does_not_call_Consumer() throws IOException {
        final Map<String, Object> jsonWithoutList = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        System.out.printf(jsonWithoutList.values().toString());

        createObjectUnderTest().parse(createInputStream(jsonWithoutList), eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_InputStream_with_empty_array_does_not_call_Consumer() throws IOException {
        final Map<String, List<Object>> jsonWithEmptyList = Collections.singletonMap(UUID.randomUUID().toString(), Collections.emptyList());

        createObjectUnderTest().parse(createInputStream(jsonWithEmptyList), eventConsumer);

        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_Event(final int numberOfObjects) throws IOException {
        final List<Map<String, Object>> jsonObjects = generateJsonObjectsAsList(numberOfObjects);

        createObjectUnderTest().parse(createInputStream(jsonObjects), eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfObjects)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfObjects));
        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = jsonObjects.get(i);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @ParameterizedTest
    @ArgumentsSource(JsonPermutations.class)
    void parse_with_InputStream_calls_Consumer_for_arrays_in_Json_permutations(final Function<List<Map<String, Object>>, Map<String, Object>> rootJsonGenerator) throws IOException {
        final int numberOfObjects = 10;
        final List<Map<String, Object>> jsonObjects = generateJsonObjectsAsList(numberOfObjects);

        createObjectUnderTest().parse(createInputStream(jsonObjects), eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfObjects)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfObjects));
        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = jsonObjects.get(i);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void parse_with_InputStream_calls_Consumer_with_Event_for_two_parallel_arrays(final int numberOfObjectsPerList) throws IOException {
        final List<Map<String, Object>> jsonObjectsFirst = generateJsonObjectsAsList(numberOfObjectsPerList);
        final List<Map<String, Object>> jsonObjectsSecond = generateJsonObjectsAsList(numberOfObjectsPerList);

        final Map<String, Object> rootJson = new LinkedHashMap<>();
        rootJson.put(UUID.randomUUID().toString(), jsonObjectsFirst);
        rootJson.put(UUID.randomUUID().toString(), jsonObjectsSecond);

        createObjectUnderTest().parse(createInputStream(rootJson), eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfObjectsPerList * 2)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfObjectsPerList * 2));
        final List<Map<String, Object>> expectedJsonObjects = new ArrayList<>(jsonObjectsFirst);
        expectedJsonObjects.addAll(jsonObjectsSecond);
        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            final Map<String, Object> expectedMap = expectedJsonObjects.get(i);
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));
        }
    }

    static class JsonPermutations implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments((Function<List<Map<String, Object>>, Map<String, Object>>) jsonObjects -> {
                        final Map<String, Object> deepestJson = Collections.singletonMap(UUID.randomUUID().toString(), jsonObjects);
                        final Map<String, Object> deeperJson = Collections.singletonMap(UUID.randomUUID().toString(), deepestJson);
                        return Collections.singletonMap(UUID.randomUUID().toString(), deeperJson);
                    }),
                    arguments((Function<List<Map<String, Object>>, Map<String, Object>>) jsonObjects -> {
                        final Map<String, Object> rootJson = new LinkedHashMap<>();
                        rootJson.put(UUID.randomUUID().toString(), Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
                        rootJson.put(UUID.randomUUID().toString(), jsonObjects);
                        return rootJson;
                    }),
                    arguments((Function<List<Map<String, Object>>, Map<String, Object>>) jsonObjects -> {
                        final Map<String, Object> rootJson = new LinkedHashMap<>();
                        rootJson.put(UUID.randomUUID().toString(), jsonObjects);
                        rootJson.put(UUID.randomUUID().toString(), Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
                        return rootJson;
                    }),
                    arguments((Function<List<Map<String, Object>>, Map<String, Object>>) jsonObjects -> {
                        final Map<String, Object> rootJson = new LinkedHashMap<>();
                        rootJson.put(UUID.randomUUID().toString(), Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
                        rootJson.put(UUID.randomUUID().toString(), jsonObjects);
                        rootJson.put(UUID.randomUUID().toString(), Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
                        return rootJson;
                    })
            );
        }
    }

    private InputStream createInputStream(final List<Map<String, Object>> jsonObjects) throws JsonProcessingException {
        final String keyName = UUID.randomUUID().toString();
        final Map<String, Object> jsonRoot = Collections.singletonMap(keyName, jsonObjects);
        return createInputStream(jsonRoot);
    }

    private InputStream createInputStream(final Map<String, ?> jsonRoot) throws JsonProcessingException {
        final byte[] jsonBytes = objectMapper.writeValueAsBytes(jsonRoot);

        return new ByteArrayInputStream(jsonBytes);
    }

    private static List<Map<String, Object>> generateJsonObjectsAsList(final int numberOfObjects) {
        final List<Map<String, Object>> jsonObjects = new ArrayList<>(numberOfObjects);
        for (int i = 0; i < numberOfObjects; i++)
            jsonObjects.add(generateJson());
        return Collections.unmodifiableList(jsonObjects);
    }

    private static Map<String, Object> generateJson() {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 7; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()));

        return jsonObject;
    }
}