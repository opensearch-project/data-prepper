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

class ServiceTest {

    @Test
    void testConstructorWithKeyAttributesOnly() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);

        assertEquals(keyAttributes, service.getKeyAttributes());
        assertTrue(service.getGroupByAttributes().isEmpty());
    }

    @Test
    void testConstructorWithGroupByAttributes() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Map<String, String> groupBy = Map.of("key1", "value1");
        Service service = new Service(keyAttributes, groupBy);

        assertEquals(keyAttributes, service.getKeyAttributes());
        assertEquals(groupBy, service.getGroupByAttributes());
    }

    @Test
    void testConstructorWithNullGroupByAttributes() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes, null);

        assertTrue(service.getGroupByAttributes().isEmpty());
    }

    @Test
    void testEquals() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service1 = new Service(keyAttributes);
        Service service2 = new Service(keyAttributes);

        assertEquals(service1, service2);
        assertNotEquals(service1, null);
    }

    @Test
    void testNotEquals() {
        Service.KeyAttributes keyAttributes1 = new Service.KeyAttributes("prod", "test-service1");
        Service.KeyAttributes keyAttributes2 = new Service.KeyAttributes("prod", "test-service2");
        Service service1 = new Service(keyAttributes1);
        Service service2 = new Service(keyAttributes2);

        assertNotEquals(service1, null);
        assertNotEquals(service2, null);
        assertNotEquals(service1, service2);
    }

    @Test
    void testHashCode() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service1 = new Service(keyAttributes);
        Service service2 = new Service(keyAttributes);

        assertEquals(service1.hashCode(), service2.hashCode());
    }

    @Test
    void testToString() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);

        String result = service.toString();
        assertTrue(result.contains("Service{"));
    }

    static class KeyAttributesTest {

        @Test
        void testConstructorAndGetters() {
            Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");

            assertEquals("prod", keyAttributes.getEnvironment());
            assertEquals("test-service", keyAttributes.getName());
        }

        @Test
        void testEquals() {
            Service.KeyAttributes keyAttributes1 = new Service.KeyAttributes("prod", "test-service");
            Service.KeyAttributes keyAttributes2 = new Service.KeyAttributes("prod", "test-service");

            assertEquals(keyAttributes1, keyAttributes2);
            assertNotEquals(keyAttributes1, new Service.KeyAttributes("dev", "test-service"));
            assertNotEquals(keyAttributes1, null);
        }

        @Test
        void testHashCode() {
            Service.KeyAttributes keyAttributes1 = new Service.KeyAttributes("prod", "test-service");
            Service.KeyAttributes keyAttributes2 = new Service.KeyAttributes("prod", "test-service");

            assertEquals(keyAttributes1.hashCode(), keyAttributes2.hashCode());
        }

        @Test
        void testToString() {
            Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");

            String result = keyAttributes.toString();
            assertTrue(result.contains("prod"));
            assertTrue(result.contains("test-service"));
        }
    }
}
