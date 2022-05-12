/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink.opensearch.bulk;

import jakarta.json.JsonValue;
import jakarta.json.stream.JsonGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.JsonpMapper;

import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SizedJsonDataImplTest {
    private JsonData innerJsonData;
    private long documentSize;

    @BeforeEach
    void setUp() {
        Random random = new Random();
        innerJsonData = mock(JsonData.class);
        documentSize = random.nextInt(10_000) + 100;
    }

    private SizedJsonDataImpl createObjectUnderTest() {
        return new SizedJsonDataImpl(innerJsonData, documentSize);
    }

    @Test
    void getDocumentSize_returns_the_documentSize() {
        assertThat(createObjectUnderTest().getDocumentSize(), equalTo(documentSize));
    }

    @Nested
    class ToJson {
        private JsonValue jsonValue;

        @BeforeEach
        void setUp() {
            jsonValue = mock(JsonValue.class);
        }

        @Test
        void toJson_returns_inner_JsonData_toJson() {
            when(innerJsonData.toJson()).thenReturn(jsonValue);

            assertThat(createObjectUnderTest().toJson(), equalTo(jsonValue));
        }

        @Test
        void toJson_with_mapper_returns_inner_JsonData_toJson() {
            JsonpMapper jsonpMapper = mock(JsonpMapper.class);
            when(innerJsonData.toJson(jsonpMapper)).thenReturn(jsonValue);

            assertThat(createObjectUnderTest().toJson(jsonpMapper), equalTo(jsonValue));
        }
    }

    @Nested
    class ToClass {
        private Class toClass;
        private Object expectedToObject;

        @BeforeEach
        void setUp() {
            toClass = String.class;

            expectedToObject = mock(Object.class);
        }

        @Test
        void to_returns_inner_JsonData_to() {
            when(innerJsonData.to(toClass)).thenReturn(expectedToObject);

            assertThat(createObjectUnderTest().to(toClass), equalTo(expectedToObject));
        }

        @Test
        void to_with_mapper_returns_inner_JsonData_to() {
            JsonpMapper jsonpMapper = mock(JsonpMapper.class);
            when(innerJsonData.to(toClass, jsonpMapper)).thenReturn(expectedToObject);

            assertThat(createObjectUnderTest().to(toClass, jsonpMapper), equalTo(expectedToObject));
        }
    }

    @Nested
    class Deserialize {
        private JsonpDeserializer jsonpDeserializer;
        private Object expectedDeserializedObject;

        @BeforeEach
        void setUp() {
            jsonpDeserializer = mock(JsonpDeserializer.class);
            expectedDeserializedObject = mock(Object.class);
        }

        @Test
        void deserialize_returns_inner_JsonData_deserialize() {
            when(innerJsonData.deserialize(jsonpDeserializer)).thenReturn(expectedDeserializedObject);

            assertThat(createObjectUnderTest().deserialize(jsonpDeserializer), equalTo(expectedDeserializedObject));
        }

        @Test
        void deserialize_with_mapper_returns_inner_JsonData_deserialize() {
            JsonpMapper jsonpMapper = mock(JsonpMapper.class);
            when(innerJsonData.deserialize(jsonpDeserializer, jsonpMapper)).thenReturn(expectedDeserializedObject);

            assertThat(createObjectUnderTest().deserialize(jsonpDeserializer, jsonpMapper), equalTo(expectedDeserializedObject));
        }
    }

    @Test
    void serialize_calls_inner_JsonData_serialize() {
        JsonGenerator generator = mock(JsonGenerator.class);
        JsonpMapper mapper = mock(JsonpMapper.class);

        createObjectUnderTest().serialize(generator, mapper);

        verify(innerJsonData).serialize(generator, mapper);
    }
}