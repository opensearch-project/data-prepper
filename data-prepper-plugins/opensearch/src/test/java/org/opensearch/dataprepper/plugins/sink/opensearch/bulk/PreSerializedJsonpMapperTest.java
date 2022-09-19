/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.client.json.jackson.JacksonJsonProvider;
import org.opensearch.client.json.jackson.JacksonJsonpParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PreSerializedJsonpMapperTest {
    private PreSerializedJsonpMapper createObjectUnderTest() {
        return new PreSerializedJsonpMapper();
    }

    @Test
    void jsonProvider_returns_a_non_null_JsonProvider_which_is_also_a_JacksonJsonProvider() {
        final PreSerializedJsonpMapper objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.jsonProvider(), notNullValue());
        assertThat(objectUnderTest.jsonProvider(), instanceOf(JacksonJsonProvider.class));
    }

    @Test
    void deserialize_is_able_to_create_objects() throws IOException {
        final JsonParser jsonParser = JsonFactory.builder().build().createParser("{\"a\":\"b\"}");
        final JacksonJsonpParser jacksonJsonpParser = new JacksonJsonpParser(jsonParser);
        final Map<String, Object> deserializedMap = createObjectUnderTest().deserialize(jacksonJsonpParser, Map.class);

        assertThat(deserializedMap, notNullValue());
        assertThat(deserializedMap, hasKey("a"));
        assertThat(deserializedMap.get("a"), equalTo("b"));
    }

    @Nested
    class WithSerializedJson {

        private byte[] documentBytes;
        private SerializedJson serializedJson;

        @BeforeEach
        void setUp() {
            final String notActuallyJsonString = UUID.randomUUID().toString();
            documentBytes = notActuallyJsonString.getBytes(StandardCharsets.UTF_8);
            serializedJson = mock(SerializedJson.class);
            when(serializedJson.getSerializedJson()).thenReturn(documentBytes);
        }

        @Test
        void serialize_on_SerializedJson_writes_directly_to_that_outputStream() throws IOException {
            final OutputStream outputStream = mock(OutputStream.class);

            final PreSerializedJsonpMapper objectUnderTest = createObjectUnderTest();
            final JsonGenerator generator = objectUnderTest.jsonProvider().createGenerator(outputStream);

            objectUnderTest.serialize(serializedJson, generator);

            verify(outputStream).write(documentBytes);
        }

        @Test
        void serialize_on_SerializedJson_with_an_external_JsonGenerator_throws_exception() throws IOException {
            final OutputStream outputStream = mock(OutputStream.class);

            final JsonGenerator generator = mock(JsonGenerator.class);

            final PreSerializedJsonpMapper objectUnderTest = createObjectUnderTest();

            assertThrows(IllegalArgumentException.class, () -> objectUnderTest.serialize(serializedJson, generator));
        }
    }

    @Test
    void serialize_on_Map_uses_Jackson_serializer() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final PreSerializedJsonpMapper objectUnderTest = createObjectUnderTest();
        final JsonGenerator generator = objectUnderTest.jsonProvider().createGenerator(outputStream);

        final Map document = Collections.singletonMap(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        objectUnderTest.serialize(document, generator);

        final ObjectMapper objectMapper = new ObjectMapper();
        final String expectedSerializedJson = objectMapper.writeValueAsString(document);

        assertThat(new String(outputStream.toByteArray()), equalTo(expectedSerializedJson));
    }
}