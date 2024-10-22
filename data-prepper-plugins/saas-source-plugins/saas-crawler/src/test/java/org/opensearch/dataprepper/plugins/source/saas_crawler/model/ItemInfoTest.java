package org.opensearch.dataprepper.plugins.source.saas_crawler.model;

import lombok.NonNull;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ItemInfoTest {

    static class TestItemInfo extends ItemInfo {

        public TestItemInfo(@NonNull String itemId, Map<String, String> metadata, @NonNull Long eventTime) {
            super(itemId, metadata, eventTime);
        }

        public TestItemInfo(String itemId) {
            super(itemId);
        }

        @Override
        public String getPartitionKey() {
            return "partitionKey";
        }

        @Override
        public String getId() {
            return "id";
        }

        @Override
        public Map<String, String> getKeyAttributes() {
            return Map.of();
        }
    }

    @Test
    void testItemInfoSimpleConstructor() {
        String itemId = UUID.randomUUID().toString();
        TestItemInfo itemInfo = new TestItemInfo(itemId);
        assert itemInfo.itemId.equals(itemId);
        assert itemInfo.metadata == null;
        assert itemInfo.getPartitionKey().equals("partitionKey");
        assert itemInfo.getId().equals("id");
        assert itemInfo.getKeyAttributes().isEmpty();
    }

    @Test
    void testItemInfoWithNullValues() {
        assertThrows(NullPointerException.class, () -> new TestItemInfo(null, null, null));
        assertThrows(NullPointerException.class, () -> new TestItemInfo(null, null, 1234L),
                "itemId should not be null");
        assertThrows(NullPointerException.class, () -> new TestItemInfo("itemid", null, null),
                "eventTime should not be null");

    }

    @Test
    void testItemInfo() {
        String itemId = UUID.randomUUID().toString();
        TestItemInfo itemInfo = new TestItemInfo(itemId, Map.of("k1", "v1"), 1L);

        assert itemInfo.itemId.equals(itemId);
        assert !itemInfo.metadata.isEmpty();
        assert itemInfo.getMetadata().get("k1").equals("v1");
        assert itemInfo.getEventTime() == 1L;
        assert itemInfo.getPartitionKey().equals("partitionKey");
        assert itemInfo.getId().equals("id");
        assert itemInfo.getKeyAttributes().isEmpty();

        //Modify a few fields
        itemInfo.setEventTime(1234L);
        itemInfo.setItemId("updatedItemId");
        itemInfo.setMetadata(Map.of("k2", "v2"));
        assert itemInfo.getEventTime() == 1234L;
        assert itemInfo.itemId.equals("updatedItemId");
        assert itemInfo.getMetadata().get("k2").equals("v2");

    }
}
