/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeOperationDetailTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testConstructor_convertsInstantToIsoString() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        assertNotNull(detail.getTimestamp());
        assertEquals("2021-01-01T00:00:00Z", detail.getTimestamp());
    }

    @Test
    void testNodeConnectionOnly_hasNodeHashButNoOperationHash() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        assertNotNull(detail.getNodeConnectionHash());
        assertNull(detail.getOperationConnectionHash());
        assertNull(detail.getSourceOperation());
        assertNull(detail.getTargetOperation());
    }

    @Test
    void testWithOperations_hasBothHashes() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");
        Operation sourceOp = new Operation("GET /api/users");
        Operation targetOp = new Operation("GET /users");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, sourceOp, targetOp, testInstant);

        assertNotNull(detail.getNodeConnectionHash());
        assertNotNull(detail.getOperationConnectionHash());
        assertEquals(sourceOp, detail.getSourceOperation());
        assertEquals(targetOp, detail.getTargetOperation());
    }

    @Test
    void testEquals_withSameData() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail1 = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);
        NodeOperationDetail detail2 = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        assertEquals(detail1, detail2);
    }

    @Test
    void testHashCode_withSameData() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail1 = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);
        NodeOperationDetail detail2 = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        assertEquals(detail1.hashCode(), detail2.hashCode());
    }

    @Test
    void testJsonSerialization_withOperations() throws Exception {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("database", "prod", "user-postgres");
        Operation sourceOp = new Operation("GET /api/users");
        Operation targetOp = new Operation("SELECT * FROM users");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, sourceOp, targetOp, testInstant);

        String json = OBJECT_MAPPER.writeValueAsString(detail);

        assertNotNull(json);
        assertTrue(json.contains("\"timestamp\":\"2021-01-01T00:00:00Z\""));
        assertTrue(json.contains("\"sourceNode\""));
        assertTrue(json.contains("\"targetNode\""));
        assertTrue(json.contains("\"sourceOperation\""));
        assertTrue(json.contains("\"targetOperation\""));
        assertTrue(json.contains("\"nodeConnectionHash\""));
        assertTrue(json.contains("\"operationConnectionHash\""));
    }

    @Test
    void testJsonSerialization_withoutOperations_excludesNulls() throws Exception {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        String json = OBJECT_MAPPER.writeValueAsString(detail);

        assertNotNull(json);
        assertTrue(json.contains("\"nodeConnectionHash\""));
        // operationConnectionHash is null, should not appear due to @JsonInclude(NON_NULL)
        assertTrue(!json.contains("\"operationConnectionHash\""));
        assertTrue(!json.contains("\"sourceOperation\""));
        assertTrue(!json.contains("\"targetOperation\""));
    }

    @Test
    void testToString_containsTimestamp() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        String toString = detail.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("timestamp='2021-01-01T00:00:00Z'"));
    }

    @Test
    void testLeafNode_withNullTarget() {
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Operation sourceOp = new Operation("GET /api/users");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, null, sourceOp, null, testInstant);

        assertNotNull(detail.getNodeConnectionHash());
        assertNotNull(detail.getOperationConnectionHash());
        assertNull(detail.getTargetNode());
        assertNull(detail.getTargetOperation());
    }

    @Test
    void testTimestamp_isInIsoFormat() {
        Instant testInstant = Instant.now();
        Node sourceNode = createTestNode("service", "prod", "service-a");
        Node targetNode = createTestNode("service", "prod", "service-b");

        NodeOperationDetail detail = new NodeOperationDetail(sourceNode, targetNode, null, null, testInstant);

        String timestamp = detail.getTimestamp();
        assertTrue(timestamp.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z"),
                "Timestamp should be in ISO-8601 format: " + timestamp);
    }

    private Node createTestNode(String type, String environment, String name) {
        return new Node(type, new Node.KeyAttributes(environment, name));
    }
}
