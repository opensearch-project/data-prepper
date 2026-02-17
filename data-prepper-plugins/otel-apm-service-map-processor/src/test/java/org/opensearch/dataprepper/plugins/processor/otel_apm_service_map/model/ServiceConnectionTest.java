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
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServiceConnectionTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testConstructor_convertsInstantToIsoString() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");

        // When
        ServiceConnection connection = new ServiceConnection(service, remoteService, testInstant);

        // Then
        assertNotNull(connection.getTimestamp());
        assertEquals("2021-01-01T00:00:00Z", connection.getTimestamp());
    }

    @Test
    void testGetTimestamp_returnsIsoFormattedString() {
        // Given
        Instant testInstant = Instant.parse("2023-05-15T10:30:45.123Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");

        // When
        ServiceConnection connection = new ServiceConnection(service, remoteService, testInstant);

        // Then
        String timestamp = connection.getTimestamp();
        assertNotNull(timestamp);
        assertEquals("2023-05-15T10:30:45.123Z", timestamp);
    }

    @Test
    void testTimestamp_isInIsoFormat() {
        // Given
        Instant testInstant = Instant.now();
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");

        // When
        ServiceConnection connection = new ServiceConnection(service, remoteService, testInstant);

        // Then
        String timestamp = connection.getTimestamp();
        // ISO format pattern: yyyy-MM-ddTHH:mm:ss.SSSZ or yyyy-MM-ddTHH:mm:ssZ or with nanoseconds
        assertTrue(timestamp.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z"),
                "Timestamp should be in ISO-8601 format: " + timestamp);
    }

    @Test
    void testEquals_withSameTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");

        // When
        ServiceConnection connection1 = new ServiceConnection(service, remoteService, testInstant);
        ServiceConnection connection2 = new ServiceConnection(service, remoteService, testInstant);

        // Then
        assertEquals(connection1, connection2);
    }

    @Test
    void testHashCode_withSameTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");

        // When
        ServiceConnection connection1 = new ServiceConnection(service, remoteService, testInstant);
        ServiceConnection connection2 = new ServiceConnection(service, remoteService, testInstant);

        // Then
        assertEquals(connection1.hashCode(), connection2.hashCode());
    }

    @Test
    void testJsonSerialization() throws Exception {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");
        ServiceConnection connection = new ServiceConnection(service, remoteService, testInstant);

        // When
        String json = OBJECT_MAPPER.writeValueAsString(connection);

        // Then
        assertNotNull(json);
        assertTrue(json.contains("\"timestamp\":\"2021-01-01T00:00:00Z\""));
    }

    @Test
    void testToString_containsTimestamp() {
        // Given
        Instant testInstant = Instant.parse("2021-01-01T00:00:00Z");
        Service service = createTestService("prod", "service-a");
        Service remoteService = createTestService("prod", "service-b");
        ServiceConnection connection = new ServiceConnection(service, remoteService, testInstant);

        // When
        String toString = connection.toString();

        // Then
        assertNotNull(toString);
        assertTrue(toString.contains("timestamp=2021-01-01T00:00:00Z"));
    }

    private Service createTestService(String environment, String name) {
        return new Service(new Service.KeyAttributes(environment, name));
    }
}
