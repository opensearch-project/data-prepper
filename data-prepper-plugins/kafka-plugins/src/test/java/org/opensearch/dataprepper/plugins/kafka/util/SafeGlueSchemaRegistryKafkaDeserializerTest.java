/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SafeGlueSchemaRegistryKafkaDeserializerTest {

    private static final String TOPIC = "test-topic";
    private static final byte[] RAW_DATA = new byte[]{0x03, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static final byte[] JSON_PAYLOAD = "{\"orderId\":\"ABC-123\",\"amount\":99.5}".getBytes(StandardCharsets.UTF_8);

    @Mock
    private GlueSchemaRegistryKafkaDeserializer delegate;

    @Mock
    private GlueSchemaRegistryDeserializationFacade facade;

    @Mock
    private Schema schema;

    private SafeGlueSchemaRegistryKafkaDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new SafeGlueSchemaRegistryKafkaDeserializer(delegate);
    }

    @Test
    void deserialize_withNullData_returnsNull() {
        final Object result = deserializer.deserialize(TOPIC, null);
        assertThat(result, is(nullValue()));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withJsonSchema_returnsMapAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn("JSON");
        when(facade.getActualData(RAW_DATA)).thenReturn(JSON_PAYLOAD);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        final Map<String, Object> map = (Map<String, Object>) result;
        assertThat(map.get("orderId"), is("ABC-123"));
        assertThat(map.get("amount"), is(99.5));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withNullSchema_parsesAsMapAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(null);
        when(facade.getActualData(RAW_DATA)).thenReturn(JSON_PAYLOAD);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, instanceOf(Map.class));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withNullDataFormat_parsesAsMapAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn(null);
        when(facade.getActualData(RAW_DATA)).thenReturn(JSON_PAYLOAD);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, instanceOf(Map.class));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_whenSchemaFetchThrows_parsesAsMapAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenThrow(new RuntimeException("connection error"));
        when(facade.getActualData(RAW_DATA)).thenReturn(JSON_PAYLOAD);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, instanceOf(Map.class));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_whenFacadeAccessThrows_parsesAsMapAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade())
                .thenThrow(new RuntimeException("facade unavailable"))
                .thenReturn(facade);
        when(facade.getActualData(RAW_DATA)).thenReturn(JSON_PAYLOAD);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, instanceOf(Map.class));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withJsonSchema_whenGetActualDataThrows_returnsNullAndNeverDelegates() {
        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn("JSON");
        when(facade.getActualData(RAW_DATA)).thenThrow(new RuntimeException("payload extraction failed"));

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, is(nullValue()));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withJsonSchema_whenPayloadIsInvalidJson_returnsNullAndNeverDelegates() {
        final byte[] invalidJson = "not valid json {{{".getBytes(StandardCharsets.UTF_8);

        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn("JSON");
        when(facade.getActualData(RAW_DATA)).thenReturn(invalidJson);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, is(nullValue()));
        verify(delegate, never()).deserialize(any(), any());
    }

    @Test
    void deserialize_withAvroSchema_delegatesToGlue() {
        final Object expected = new Object();

        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn("AVRO");
        when(delegate.deserialize(TOPIC, RAW_DATA)).thenReturn(expected);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, is(expected));
        verify(delegate).deserialize(TOPIC, RAW_DATA);
    }

    @Test
    void deserialize_withProtobufSchema_delegatesToGlue() {
        final Object expected = new Object();

        when(delegate.getGlueSchemaRegistryDeserializationFacade()).thenReturn(facade);
        when(facade.getSchema(RAW_DATA)).thenReturn(schema);
        when(schema.getDataFormat()).thenReturn("PROTOBUF");
        when(delegate.deserialize(TOPIC, RAW_DATA)).thenReturn(expected);

        final Object result = deserializer.deserialize(TOPIC, RAW_DATA);

        assertThat(result, is(expected));
        verify(delegate).deserialize(TOPIC, RAW_DATA);
    }

    @Test
    void close_delegatesToGlue() {
        deserializer.close();
        verify(delegate).close();
    }

    @Test
    void configure_delegatesToGlue() {
        final Map<String, Object> configs = Map.of("key", "value");
        deserializer.configure(configs, false);
        verify(delegate).configure(configs, false);
    }
}
