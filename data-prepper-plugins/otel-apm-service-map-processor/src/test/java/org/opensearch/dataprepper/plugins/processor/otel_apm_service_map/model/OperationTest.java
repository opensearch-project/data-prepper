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

class OperationTest {

    @Test
    void testConstructorWithNameOnly() {
        Operation operation = new Operation("test-operation");

        assertEquals("test-operation", operation.getName());
        assertTrue(operation.getAttributes().isEmpty());
    }

    @Test
    void testConstructorWithAttributes() {
        Map<String, String> attributes = Map.of("http.method", "GET");
        Operation operation = new Operation("test-operation", attributes);

        assertEquals("test-operation", operation.getName());
        assertEquals(attributes, operation.getAttributes());
    }

    @Test
    void testConstructorWithNullAttributes() {
        Operation operation = new Operation("test-operation", null);

        assertTrue(operation.getAttributes().isEmpty());
    }

    @Test
    void testEquals() {
        Operation operation1 = new Operation("test-operation");
        Operation operation2 = new Operation("test-operation");

        assertEquals(operation1, operation2);
        assertNotEquals(operation1, new Operation("different"));
        assertNotEquals(operation1, null);
    }

    @Test
    void testHashCode() {
        Operation operation1 = new Operation("test-operation");
        Operation operation2 = new Operation("test-operation");

        assertEquals(operation1.hashCode(), operation2.hashCode());
    }

    @Test
    void testToString() {
        Operation operation = new Operation("test-operation");

        String result = operation.toString();
        assertTrue(result.contains("test-operation"));
    }
}
