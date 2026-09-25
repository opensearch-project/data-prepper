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

    @Test
    void testDependencyAttributesDefaultsEmpty() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "test-service");

        assertTrue(new Node("service", keyAttributes).getDependencyAttributes().isEmpty());
        assertTrue(new Node("service", keyAttributes, Map.of("k", "v")).getDependencyAttributes().isEmpty());
    }

    @Test
    void testConstructorWithDependencyAttributes() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("generic:default", "postgresql");
        Map<String, String> dependencyAttributes =
                Map.of("db.system.name", "postgresql", "server.address", "db-host", "server.port", "5432");
        Node node = new Node("database", keyAttributes, Map.of(), dependencyAttributes);

        assertEquals("database", node.getType());
        assertEquals(dependencyAttributes, node.getDependencyAttributes());
    }

    @Test
    void testConstructorWithNullDependencyAttributes() {
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("generic:default", "postgresql");
        Node node = new Node("database", keyAttributes, null, null);

        assertTrue(node.getDependencyAttributes().isEmpty());
    }

    @Test
    void testEquals_ignoresDependencyAttributes() {
        // dependencyAttributes is descriptive metadata, not identity: nodes differing only in it are
        // equal and hash equally, keeping the topology node shared and connection hashes stable.
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("generic:default", "postgresql");
        Node node1 = new Node("database", keyAttributes, Map.of(), Map.of("server.port", "5432"));
        Node node2 = new Node("database", keyAttributes, Map.of(), Map.of("server.port", "5433"));

        assertEquals(node1, node2);
        assertEquals(node1.hashCode(), node2.hashCode());
    }

    @Test
    void testHashCode_unchangedByEmptyDependencyAttributes() {
        // Guards the upgrade path: appending an (empty) dependencyAttributes must not shift the hash
        // of an existing service node, or every nodeConnectionHash would change on rollout.
        Node.KeyAttributes keyAttributes = new Node.KeyAttributes("prod", "checkout");
        Node threeArg = new Node("service", keyAttributes, Map.of());
        Node fourArg = new Node("service", keyAttributes, Map.of(), Map.of());

        assertEquals(threeArg.hashCode(), fourArg.hashCode());
        assertEquals(threeArg, fourArg);
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
