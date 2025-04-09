/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

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
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.NoneDecompressionEngine;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JsonInputCodecTest {

    private ObjectMapper objectMapper;
    private JsonInputCodecConfig jsonInputCodecConfig;
    private Consumer<Record<Event>> eventConsumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        jsonInputCodecConfig = mock(JsonInputCodecConfig.class);
        when(jsonInputCodecConfig.getIncludeKeysMetadata()).thenReturn(null);
        when(jsonInputCodecConfig.getIncludeKeys()).thenReturn(null);
        when(jsonInputCodecConfig.getKeyName()).thenReturn(null);
        when(jsonInputCodecConfig.getMaxEventLength()).thenReturn(null);
        eventConsumer = mock(Consumer.class);
    }

    private JsonInputCodec createObjectUnderTest() {
        return new JsonInputCodec(jsonInputCodecConfig);
    }

    @Test
    void parse_with_null_InputStream_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse((InputStream) null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_InputStream_null_Consumer_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

        final InputStream inputStream = mock(InputStream.class);
        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse(inputStream, null));

        verifyNoInteractions(inputStream);
    }

    @Test
    void parse_with_null_InputFile_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse((InputFile) null, new NoneDecompressionEngine(), eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_InputFile_null_Consumer_throws() {
        final JsonInputCodec objectUnderTest = createObjectUnderTest();

        final InputFile inputFile = mock(InputFile.class);
        assertThrows(NullPointerException.class, () ->
                objectUnderTest.parse(inputFile, new NoneDecompressionEngine(), null));

        verifyNoInteractions(inputFile);
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
    void parse_with_InputFile_calls_Consumer_with_Event(final int numberOfObjects) throws IOException {
        final List<Map<String, Object>> jsonObjects = generateJsonObjectsAsList(numberOfObjects);
        final InputStream inputStream = createInputStream(jsonObjects);

        // write inputSteam to a file
        byte[] buffer = new byte[1024];
        int bytesRead;

        File testDataFile = File.createTempFile("JsonCodecTest-" + numberOfObjects, ".json");
        testDataFile.deleteOnExit();

        OutputStream outStream = new FileOutputStream(testDataFile);
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, bytesRead);
        }
        outStream.flush();
        outStream.close();

        final InputFile inputFile = new LocalInputFile(testDataFile);

        createObjectUnderTest().parse(inputFile, new NoneDecompressionEngine(), eventConsumer);

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
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_validKey(final int numberOfObjects) throws IOException {
        parse_InputStream_withEventConfig(numberOfObjects, "key", Collections.emptyList(), Collections.emptyList());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_validKey_includeKeys(final int numberOfObjects) throws IOException {
        List<String> includeKeys = new ArrayList<>();
        for (int i=0; i<numberOfObjects; i++) {
            includeKeys.add(UUID.randomUUID().toString());
        }
        parse_InputStream_withEventConfig(numberOfObjects, "key", includeKeys, Collections.emptyList());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_validKey_includeKeys_and_MetadataKeys(final int numberOfObjects) throws IOException {
        List<String> includeKeys = new ArrayList<>();
        List<String> includeMetadataKeys = new ArrayList<>();
        for (int i=0; i<numberOfObjects; i++) {
            includeKeys.add(UUID.randomUUID().toString());
            includeMetadataKeys.add(UUID.randomUUID().toString());
        }
        parse_InputStream_withEventConfig(numberOfObjects, "key", includeKeys, includeMetadataKeys);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_validKey_includeMetadataKeys(final int numberOfObjects) throws IOException {
        List<String> includeMetadataKeys = new ArrayList<>();
        for (int i=0; i<numberOfObjects; i++) {
            includeMetadataKeys.add(UUID.randomUUID().toString());
        }
        parse_InputStream_withEventConfig(numberOfObjects, "key", Collections.emptyList(), includeMetadataKeys);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_notMatchKey(final int numberOfObjects) throws IOException {
        List<String> includeMetadataKeys = null;
        List<String> includeKeys = null;
        final Map<String, Object> jsonObjects = generateJsonWithSpecificKeys(includeKeys, includeMetadataKeys, "key", numberOfObjects, 2);
        when(jsonInputCodecConfig.getIncludeKeysMetadata()).thenReturn(includeMetadataKeys);
        when(jsonInputCodecConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(jsonInputCodecConfig.getKeyName()).thenReturn("key2");

        createObjectUnderTest().parse(createInputStream(jsonObjects), eventConsumer);

        verify(eventConsumer, times(0)).accept(any(Record.class));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_EventConfig_NullKey(final int numberOfObjects) throws IOException {
        List<String> includeMetadataKeys = null;
        List<String> includeKeys = null;
        final Map<String, Object> jsonObjects = generateJsonWithSpecificKeys(includeKeys, includeMetadataKeys, "key", numberOfObjects, 2);
        when(jsonInputCodecConfig.getIncludeKeysMetadata()).thenReturn(includeMetadataKeys);
        when(jsonInputCodecConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(jsonInputCodecConfig.getKeyName()).thenReturn(null);

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

    private Map<String, Object> generateJsonWithSpecificKeys(final List<String> includeKeys,
                                                             final List<String> includeMetadataKeys,
                                                             final String key,
                                                             final int numKeyRecords,
                                                             final int numKeyPerRecord) {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        final List<Map<String, Object>> innerObjects = new ArrayList<>();

        if (includeKeys != null && !includeKeys.isEmpty()) {
            for (String includeKey : includeKeys) {
                jsonObject.put(includeKey, UUID.randomUUID().toString());
            }
        }

        if (includeMetadataKeys != null && !includeMetadataKeys.isEmpty()) {
            for (String includeMetadataKey : includeMetadataKeys) {
                jsonObject.put(includeMetadataKey, UUID.randomUUID().toString());
            }
        }

        for (int i=0; i<numKeyRecords; i++) {
            final Map<String, Object> innerJsonMap = new LinkedHashMap<>();
            for (int j=0; j<numKeyPerRecord; j++) {
                innerJsonMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            }
            innerObjects.add(innerJsonMap);
        }
        jsonObject.put(key, innerObjects);
        return jsonObject;
    }

    private void parse_InputStream_withEventConfig(final int numberOfObjects, final String objectKey, final List<String> includeKeys, final List<String> includeMetadataKeys) throws IOException {
        final Map<String, Object> jsonObjects = generateJsonWithSpecificKeys(includeKeys, includeMetadataKeys, objectKey, numberOfObjects, 2);
        when(jsonInputCodecConfig.getIncludeKeysMetadata()).thenReturn(includeMetadataKeys);
        when(jsonInputCodecConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(jsonInputCodecConfig.getKeyName()).thenReturn(objectKey);

        createObjectUnderTest().parse(createInputStream(jsonObjects), eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfObjects)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertThat(actualRecords.size(), equalTo(numberOfObjects));
        for (final Record<Event> actualRecord : actualRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());

            Map<String, Object> dataMap = actualRecord.getData().toMap();
            for (String includeKey : includeKeys) {
                assertTrue(dataMap.containsKey(includeKey));
            }

            Map<String, Object> metadataMap = actualRecord.getData().getMetadata().getAttributes();
            for (String includeMetadataKey: includeMetadataKeys) {
                assertTrue(metadataMap.containsKey(includeMetadataKey));
            }
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }
    }
}