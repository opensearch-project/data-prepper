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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeTest {

    @Test
    void testConstructorWithKeyAttributesOnly() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node = new Node("service", keyAttributes);

        assertEquals("service", node.getType());
        assertEquals(keyAttributes, node.getKeyAttributes());
        assertTrue(node.getGroupByAttributes().isEmpty());
    }

    @Test
    void testConstructorWithGroupByAttributes() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Map<String, String> groupBy = Map.of("key1", "value1");
        Node node = new Node("service", keyAttributes, groupBy);

        assertEquals("service", node.getType());
        assertEquals(keyAttributes, node.getKeyAttributes());
        assertEquals(groupBy, node.getGroupByAttributes());
    }

    @Test
    void testConstructorWithNullGroupByAttributes() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node = new Node("service", keyAttributes, null);

        assertTrue(node.getGroupByAttributes().isEmpty());
    }

    @Test
    void testEquals() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node1 = new Node("service", keyAttributes);
        Node node2 = new Node("service", keyAttributes);

        assertEquals(node1, node2);
        assertNotEquals(node1, null);
    }

    @Test
    void testNotEquals_differentType() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node1 = new Node("service", keyAttributes);
        Node node2 = new Node("database", keyAttributes);

        assertNotEquals(node1, node2);
    }

    @Test
    void testNotEquals_differentName() {
        Node.KeyAttributes keyAttributes1 = new Node.KeyAttributes("prod", "test-service1");
        Node.KeyAttributes keyAttributes2 = new Node.KeyAttributes("prod", "test-service2");
        Node node1 = new Node("service", keyAttributes1);
        Node node2 = new Node("service", keyAttributes2);

        assertNotEquals(node1, node2);
    }

    @Test
    void testHashCode() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node1 = new Node("service", keyAttributes);
        Node node2 = new Node("service", keyAttributes);

        assertEquals(node1.hashCode(), node2.hashCode());
    }

    @Test
    void testToString() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");
        Node node = new Node("service", keyAttributes);

        String result = node.toString();
        assertTrue(result.contains("Node{"));
        assertTrue(result.contains("service"));
    }

    static class KeyAttributesTest {

        @Test
        void testConstructorAndGetters() {
            Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");

            assertEquals("prod", keyAttributes.getEnvironment());
            assertEquals("test-service", keyAttributes.getName());
        }

        @Test
        void testEquals() {
            Node.KeyAttributes keyAttributes1 = new Node.KeyAttributes("prod", "test-service");
            Node.KeyAttributes keyAttributes2 = new Node.KeyAttributes("prod", "test-service");

            assertEquals(keyAttributes1, keyAttributes2);
            assertNotEquals(keyAttributes1, new Node.KeyAttributes("dev", "test-service"));
            assertNotEquals(keyAttributes1, null);
        }

        @Test
        void testHashCode() {
            Node.KeyAttributes keyAttributes1 = new Node.KeyAttributes("prod", "test-service");
            Node.KeyAttributes keyAttributes2 = new Node.KeyAttributes("prod", "test-service");

            assertEquals(keyAttributes1.hashCode(), keyAttributes2.hashCode());
        }

        @Test
        void testToString() {
            Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");

            String result = keyAttributes.toString();
            assertTrue(result.contains("prod"));
            assertTrue(result.contains("test-service"));
        }
    }
}
