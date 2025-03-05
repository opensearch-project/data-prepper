package org.opensearch.dataprepper.plugins.source.source_crawler.model;

import java.time.Instant;
import java.util.Map;

public class TestItemInfo implements ItemInfo {

    String itemId;
    Map<String, Object> metadata;
    Instant eventTime;

    public TestItemInfo(String itemId, Map<String, Object> metadata, Instant eventTime) {
        this.itemId = itemId;
        this.metadata = metadata;
        this.eventTime = eventTime;
    }

    public TestItemInfo(String itemId) {
        this.itemId = itemId;
        this.metadata = Map.of();
    }

    @Override
    public String getItemId() {
        return itemId;
    }

    @Override
    public Map<String, Object> getMetadata() {
        return this.metadata;
    }

    @Override
    public Instant getEventTime() {
        return this.eventTime;
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
    public Map<String, Object> getKeyAttributes() {
        return Map.of();
    }

    @Override
    public Instant getLastModifiedAt() {
        return Instant.ofEpochMilli(10);
    }
}
