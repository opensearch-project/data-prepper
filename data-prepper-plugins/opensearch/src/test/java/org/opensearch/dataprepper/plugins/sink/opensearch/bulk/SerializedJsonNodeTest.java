/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.RandomStringUtils;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class SerializedJsonNodeTest {
    private int documentSize;
    private byte[] documentBytes;
    private String documentId;
    private String routingField;
    private JsonNode jsonNode;
    private SerializedJson document;
    private String jsonString;

    @BeforeEach
    void setUp() {
        Random random = new Random();
        jsonString = "{\"key\":\"value\"}";
        documentSize = jsonString.length();

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            jsonNode = objectMapper.readTree(jsonString);
        } catch (Exception e) {
            jsonNode = null;
        }
	    documentId = RandomStringUtils.randomAlphabetic(10);
	    routingField = RandomStringUtils.randomAlphabetic(10);
        document = SerializedJson.fromStringAndOptionals(jsonString, documentId, routingField);
    }

    private SerializedJsonNode createObjectUnderTest() {
        return new SerializedJsonNode(jsonNode, document);
    }

    @Test
    void getDocumentSize_returns_size_of_the_document_byte_array() {
        assertThat(createObjectUnderTest().getDocumentSize(), equalTo((long) documentSize));
    }

    @Test
    void getSerializedJson_returns_the_document_byte_array_and_fields() {
        assertThat(createObjectUnderTest().getSerializedJson(), equalTo(jsonString.getBytes()));
        assertThat(createObjectUnderTest().getDocumentId().get(), equalTo(documentId));
        assertThat(createObjectUnderTest().getRoutingField().get(), equalTo(routingField));
    }
}

