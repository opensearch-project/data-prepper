/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public boolean isJsonNodeDifferent(final JsonNode oldNode,final JsonNode newNode) {
        if (oldNode.isObject() && newNode.isObject()) {
            final ObjectNode objNode1 = (ObjectNode) oldNode;
            final ObjectNode objNode2 = (ObjectNode) newNode;
            return !objNode1.equals(objNode2);
        }
        return !oldNode.equals(newNode);
    }

    public JsonNode getJsonNode(final String value) throws JsonProcessingException {
        return objectMapper.readTree(value);
    }

    public <T> T getReadValue(final InputStream value, final TypeReference<T> type) throws IOException {
        return objectMapper.readValue(value, type);
    }

    public <T> T getReadValue(final File value, final Class<T> type) throws IOException {
        return objectMapper.readValue(value, type);
    }

    public String getJsonValue(final Object value) throws JsonProcessingException {
        return objectMapper.writeValueAsString(value);
    }
}
