/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.configuration.Office365ItemInfo;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Office365ItemInfoTest {

    @Test
    void testBuilderAndGetters() {
        // Setup test data
        String itemId = "test-id";
        Instant eventTime = Instant.now();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        String partitionKey = "partition1";
        Map<String, Object> keyAttributes = new HashMap<>();
        keyAttributes.put("key2", "value2");
        Instant lastModifiedAt = Instant.now();

        // Create Office365ItemInfo using builder
        Office365ItemInfo itemInfo = Office365ItemInfo.builder()
                .itemId(itemId)
                .eventTime(eventTime)
                .metadata(metadata)
                .partitionKey(partitionKey)
                .keyAttributes(keyAttributes)
                .lastModifiedAt(lastModifiedAt)
                .build();

        // Verify all getters
        assertNotNull(itemInfo, "ItemInfo should not be null");
        assertEquals(itemId, itemInfo.getItemId(), "ItemId should match");
        assertEquals(itemId, itemInfo.getId(), "Id should match itemId");
        assertEquals(eventTime, itemInfo.getEventTime(), "EventTime should match");
        assertEquals(metadata, itemInfo.getMetadata(), "Metadata should match");
        assertEquals(partitionKey, itemInfo.getPartitionKey(), "PartitionKey should match");
        assertEquals(keyAttributes, itemInfo.getKeyAttributes(), "KeyAttributes should match");
        assertEquals(lastModifiedAt, itemInfo.getLastModifiedAt(), "LastModifiedAt should match");
    }

    @Test
    void testWithNullValues() {
        // Create Office365ItemInfo with null values
        Office365ItemInfo itemInfo = Office365ItemInfo.builder()
                .itemId(null)
                .eventTime(null)
                .metadata(null)
                .partitionKey(null)
                .keyAttributes(null)
                .lastModifiedAt(null)
                .build();

        // Verify null values are handled
        assertNotNull(itemInfo, "ItemInfo should not be null even with null values");
        assertEquals(null, itemInfo.getItemId());
        assertEquals(null, itemInfo.getId());
        assertEquals(null, itemInfo.getEventTime());
        assertEquals(null, itemInfo.getMetadata());
        assertEquals(null, itemInfo.getPartitionKey());
        assertEquals(null, itemInfo.getKeyAttributes());
        assertEquals(null, itemInfo.getLastModifiedAt());
    }

    @Test
    void testWithEmptyMaps() {
        // Create Office365ItemInfo with empty maps
        Office365ItemInfo itemInfo = Office365ItemInfo.builder()
                .itemId("test-id")
                .eventTime(Instant.now())
                .metadata(new HashMap<>())
                .partitionKey("partition1")
                .keyAttributes(new HashMap<>())
                .lastModifiedAt(Instant.now())
                .build();

        // Verify empty maps
        assertNotNull(itemInfo.getMetadata(), "Metadata should not be null");
        assertEquals(0, itemInfo.getMetadata().size(), "Metadata should be empty");
        assertNotNull(itemInfo.getKeyAttributes(), "KeyAttributes should not be null");
        assertEquals(0, itemInfo.getKeyAttributes().size(), "KeyAttributes should be empty");
    }
}
