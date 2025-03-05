package org.opensearch.dataprepper.plugins.source.source_crawler.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ItemInfoTest {

    @Test
    void testItemInfoSimpleConstructor() {
        String itemId = UUID.randomUUID().toString();
        TestItemInfo itemInfo = new TestItemInfo(itemId);
        assertEquals(itemId, itemInfo.getItemId());
        assertEquals(0, itemInfo.getMetadata().size());
        assertEquals("partitionKey", itemInfo.getPartitionKey());
        assertEquals("id", itemInfo.getId());
        assertTrue(itemInfo.getKeyAttributes().isEmpty());
    }

    @Test
    void testItemInfo() {
        String itemId = UUID.randomUUID().toString();
        TestItemInfo itemInfo = new TestItemInfo(itemId, Map.of("k1", "v1"), Instant.ofEpochMilli(1L));

        assertEquals(itemId, itemInfo.getItemId());
        assertFalse(itemInfo.getMetadata().isEmpty());
        assertEquals("v1", itemInfo.getMetadata().get("k1"));
        assertEquals(Instant.ofEpochMilli(1L), itemInfo.getEventTime());
        assertEquals("partitionKey", itemInfo.getPartitionKey());
        assertEquals("id", itemInfo.getId());
        assertTrue(itemInfo.getKeyAttributes().isEmpty());

    }
}
