/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
public class JsonUtilsTest {

    private JsonUtils jsonUtils = new JsonUtils();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String json = "{\"Year\":\"2011\",\"Age\":\"25\",\"Ethnic\":\"4\"}";

    @Test
    public void isJsonNodeDifferentTest() throws JsonProcessingException {
        JsonNode oldNode = objectMapper.readTree(json);
        final String newJson = "{\"Year\":\"2011\",\"Age\":\"25\"}";
        JsonNode newNode = objectMapper.readTree(newJson);
        Assertions.assertTrue(jsonUtils.isJsonNodeDifferent(oldNode, newNode));

    }

    @Test
    public void getJsonNodeTest() throws JsonProcessingException {
        Assertions.assertNotNull(jsonUtils.getJsonNode(json));
    }

    @Test
    public void getJsonValueTest() throws JsonProcessingException {
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("type", "string");
        Assertions.assertNotNull(jsonUtils.getJsonValue(inputMap));
    }

    @Test
    public void getReadValueInputStreamTest() throws IOException {
        Assertions.assertNotNull(jsonUtils.getReadValue(new ByteArrayInputStream(json.getBytes()), new TypeReference<Object>() {
        }));
    }

}
