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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationTest {

    @Test
    void testConstructorAndGetters() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);
        Operation operation = new Operation("test-operation", service, "remote-op");

        assertEquals("test-operation", operation.getName());
        assertEquals(service, operation.getRemoteService());
        assertEquals("remote-op", operation.getRemoteOperationName());
    }

    @Test
    void testEquals() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);
        Operation operation1 = new Operation("test-operation", service, "remote-op");
        Operation operation2 = new Operation("test-operation", service, "remote-op");

        assertEquals(operation1, operation2);
        assertNotEquals(operation1, new Operation("different", service, "remote-op"));
        assertNotEquals(operation1, null);
    }

    @Test
    void testHashCode() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);
        Operation operation1 = new Operation("test-operation", service, "remote-op");
        Operation operation2 = new Operation("test-operation", service, "remote-op");

        assertEquals(operation1.hashCode(), operation2.hashCode());
    }

    @Test
    void testToString() {
        Service.KeyAttributes keyAttributes = new Service.KeyAttributes("prod", "test-service");
        Service service = new Service(keyAttributes);
        Operation operation = new Operation("test-operation", service, "remote-op");

        String result = operation.toString();
        assertTrue(result.contains("test-operation"));
        assertTrue(result.contains("remote-op"));
    }
}
