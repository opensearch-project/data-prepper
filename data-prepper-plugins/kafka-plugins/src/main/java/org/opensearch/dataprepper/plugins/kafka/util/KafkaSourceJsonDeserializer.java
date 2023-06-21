/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;


/**
 * This class implements the deserializer for the JSON records.
 */
public class KafkaSourceJsonDeserializer implements Deserializer<JsonNode> {
    private ObjectMapper objectMapper;

    public KafkaSourceJsonDeserializer() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // nothing to configure
    }

    @Override
    public JsonNode deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readTree(data);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }

}