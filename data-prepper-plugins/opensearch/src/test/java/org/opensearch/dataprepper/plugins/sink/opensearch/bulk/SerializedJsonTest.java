/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.RandomStringUtils;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SerializedJsonTest {
    @Test
    void fromString_returns_SerializedJsonImpl() {
        assertThat(SerializedJson.fromStringAndOptionals("{}", null, null), instanceOf(SerializedJsonImpl.class));
    }

    @Test
    void fromString_throws_if_the_jsonString_is_null() {
        assertThrows(NullPointerException.class, () -> SerializedJson.fromStringAndOptionals(null, null, null));
    }

    @Test
    void fromString_returns_SerializedJsonImpl_with_correctValues() {
        String documentId = RandomStringUtils.randomAlphabetic(10);
        String routingField = RandomStringUtils.randomAlphabetic(10);
        SerializedJson serializedJson = SerializedJson.fromStringAndOptionals("{}", documentId, routingField);
            assertThat(serializedJson, instanceOf(SerializedJsonImpl.class));
        assertThat(serializedJson.getDocumentId().get(), equalTo(documentId));
        assertThat(serializedJson.getRoutingField().get(), equalTo(routingField));
        assertThat(serializedJson.getSerializedJson(), equalTo("{}".getBytes()));
    }

    @Test
    void fromString_returns_SerializedJsonNode_with_correctValues() {
        String documentId = RandomStringUtils.randomAlphabetic(10);
        String routingField = RandomStringUtils.randomAlphabetic(10);
        final String jsonString = "{\"key\":\"value\"}";
        JsonNode jsonNode;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            jsonNode = objectMapper.readTree(jsonString);
        } catch (Exception e) {
            jsonNode = null;
        }
        SerializedJson document = SerializedJson.fromStringAndOptionals(jsonString, documentId, routingField);
        SerializedJson serializedJson = SerializedJson.fromJsonNode(jsonNode, document);
        assertThat(serializedJson, instanceOf(SerializedJsonNode.class));
        assertThat(serializedJson.getDocumentId().get(), equalTo(documentId));
        assertThat(serializedJson.getRoutingField().get(), equalTo(routingField));
        assertThat(serializedJson.getSerializedJson(), equalTo(jsonString.getBytes()));
    }
}
