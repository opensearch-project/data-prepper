/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServiceOperationDetailTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testConstructor_convertsInstantToIsoString() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");

        // When
        ServiceOperationDetail detail = new ServiceOperationDetail(service, operation, testInstant);

        // Then
        assertNotNull(detail.getTimestamp());
        assertEquals("2021-01-01T00:00:00Z", detail.getTimestamp());
    }

    @Test
    void testGetTimestamp_returnsIsoFormattedString() {
        // Given
        Instant testInstant = Instant.parse("2023-05-15T10:30:45.123Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");

        // When
        ServiceOperationDetail detail = new ServiceOperationDetail(service, operation, testInstant);

        // Then
        String timestamp = detail.getTimestamp();
        assertNotNull(timestamp);
        assertEquals("2023-05-15T10:30:45.123Z", timestamp);
    }

    @Test
    void testTimestamp_isInIsoFormat() {
        // Given
        Instant testInstant = Instant.now();
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");

        // When
        ServiceOperationDetail detail = new ServiceOperationDetail(service, operation, testInstant);

        // Then
        String timestamp = detail.getTimestamp();
        // ISO format pattern: yyyy-MM-ddTHH:mm:ss.SSSZ or yyyy-MM-ddTHH:mm:ssZ or with nanoseconds
        assertTrue(timestamp.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z"),
                "Timestamp should be in ISO-8601 format: " + timestamp);
    }

    @Test
    void testEquals_withSameTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");

        // When
        ServiceOperationDetail detail1 = new ServiceOperationDetail(service, operation, testInstant);
        ServiceOperationDetail detail2 = new ServiceOperationDetail(service, operation, testInstant);

        // Then
        assertEquals(detail1, detail2);
    }

    @Test
    void testHashCode_withSameTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");

        // When
        ServiceOperationDetail detail1 = new ServiceOperationDetail(service, operation, testInstant);
        ServiceOperationDetail detail2 = new ServiceOperationDetail(service, operation, testInstant);

        // Then
        assertEquals(detail1.hashCode(), detail2.hashCode());
    }

    @Test
    void testJsonSerialization() throws Exception {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");
        ServiceOperationDetail detail = new ServiceOperationDetail(service, operation, testInstant);

        // When
        String json = OBJECT_MAPPER.writeValueAsString(detail);

        // Then
        assertNotNull(json);
        assertTrue(json.contains("\"timestamp\":\"2021-01-01T00:00:00Z\""));
    }

    @Test
    void testToString_containsTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Operation operation = createTestOperation("GET /api/users");
        ServiceOperationDetail detail = new ServiceOperationDetail(service, operation, testInstant);

        // When
        String toString = detail.toString();

        // Then
        assertNotNull(toString);
        assertTrue(toString.contains("timestamp=2021-01-01T00:00:00Z"));
    }

    private Service createTestService(String environment, String name) {
        return new Service(new Service.KeyAttributes(environment, name));
    }

    private Operation createTestOperation(String name) {
        Service remoteService = createTestService("prod", "remote-service");
        return new Operation(name, remoteService, "remote-operation");
    }
}
