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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A safe wrapper around {@link GlueSchemaRegistryKafkaDeserializer} that bypasses
 * className resolution for JSON schemas by parsing the raw payload directly as a Map.
 * <p>
 * For non-JSON schemas (AVRO, Protobuf), the delegate deserializer is used.
 */
public class SafeGlueSchemaRegistryKafkaDeserializer implements Deserializer<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(SafeGlueSchemaRegistryKafkaDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final GlueSchemaRegistryKafkaDeserializer delegate;

    SafeGlueSchemaRegistryKafkaDeserializer(final GlueSchemaRegistryKafkaDeserializer delegate) {
        this.delegate = delegate;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        delegate.configure(configs, isKey);
    }

    @Override
    public Object deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        Schema schema = null;
        try {
            final GlueSchemaRegistryDeserializationFacade facade =
                    delegate.getGlueSchemaRegistryDeserializationFacade();
            schema = facade.getSchema(data);
        } catch (final Exception e) {
            LOG.warn("Failed to fetch schema for topic '{}', parsing payload as Map: {}", topic, e.getMessage());
        }

        if (schema != null && schema.getDataFormat() != null && !"JSON".equalsIgnoreCase(schema.getDataFormat())) {
            return delegate.deserialize(topic, data);
        }

        return deserializeJson(data, topic);
    }

    private Object deserializeJson(final byte[] data, final String topic) {
        try {
            final byte[] payload = delegate.getGlueSchemaRegistryDeserializationFacade().getActualData(data);
            return OBJECT_MAPPER.readValue(payload, Map.class);
        } catch (final Exception e) {
            LOG.error("Failed to deserialize payload for topic '{}': {}", topic, e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {
        delegate.close();
    }

    GlueSchemaRegistryDeserializationFacade getGlueSchemaRegistryDeserializationFacade() {
        return delegate.getGlueSchemaRegistryDeserializationFacade();
    }
}
