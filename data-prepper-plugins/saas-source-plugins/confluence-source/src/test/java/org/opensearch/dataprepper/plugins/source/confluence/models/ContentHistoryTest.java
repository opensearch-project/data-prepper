/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.source.confluence.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ContentHistoryTest {

    ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void testGetCreatedDateInMillis_ValidDate() {
        ContentHistory history = new ContentHistory();
        history.setCreatedDate(Instant.parse("2025-02-17T23:34:44.633Z"));

        long expectedMillis = 1739835284633L; // Pre-calculated value for this timestamp
        assertEquals(expectedMillis, history.getCreatedDateInMillis());
    }

    @Test
    void testGetLastModifiedInMillis_ValidDate() {
        ContentHistory history = new ContentHistory();
        ContentHistory.LastUpdated lastUpdated = new ContentHistory.LastUpdated();
        lastUpdated.setWhen(Instant.parse("2025-02-17T23:34:44.633Z"));
        history.setLastUpdated(lastUpdated);
        long expectedMillis = 1739835284633L; // Pre-calculated value for this timestamp
        assertEquals(expectedMillis, history.getLastUpdatedInMillis());
    }

    @Test
    void testGetCreatedDateInMillis_NullDate() {
        ContentHistory history = new ContentHistory();
        history.setCreatedDate(null);
        assertEquals(0L, history.getCreatedDateInMillis());
    }

    @Test
    public void testNullValues() throws Exception {
        // Test null value
        String json = "{\"createdDate\": null, \"lastUpdated\": { \"when\": null}}";

        // Test deserialization of null
        ContentHistory deserializedData = objectMapper.readValue(json, ContentHistory.class);
        assertNull(deserializedData.getCreatedDate());
        assertNull(deserializedData.getLastUpdated().when);
    }

    @Test
    public void testNonNullValues() throws Exception {
        // Test null value
        String json = "{\"createdDate\": \"2025-02-23T23:20:20.1234z\", \"lastUpdated\": { \"when\": \"2025-02-24T23:20:20.1234z\"}}";

        // Test deserialization of null
        ContentHistory deserializedData = objectMapper.readValue(json, ContentHistory.class);
        assertEquals(Instant.parse("2025-02-23T23:20:20.123400Z"), deserializedData.getCreatedDate());
        assertEquals(Instant.parse("2025-02-24T23:20:20.123400Z"), deserializedData.getLastUpdated().when);
    }

    @Test
    public void testGetCreatedDateInMillis_InvalidDate() {
        String invalidJson = "{\"createdDate\":\"invalid-date\"}";
        assertThrows(Exception.class, () -> objectMapper.readValue(invalidJson, ContentHistory.class));
    }
}

