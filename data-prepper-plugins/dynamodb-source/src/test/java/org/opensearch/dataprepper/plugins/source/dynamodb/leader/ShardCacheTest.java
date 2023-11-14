package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class ShardCacheTest {

    @Test
    void test_Cache() {
        ShardCache shardCache = new ShardCache();
        Map<String, String> pairs = createShardPairs();
        assertThat(pairs.size(), equalTo(8)); // in case test data changed

        pairs.forEach((child, parent) -> shardCache.put(child, parent));
        // Contains 5 unique parents
        assertThat(shardCache.size(), equalTo(5));

        List<String> childShardId1 = shardCache.get("shardId-001");
        assertThat(childShardId1, notNullValue());
        assertThat(childShardId1.size(), equalTo(1));
        assertThat(childShardId1.get(0), equalTo("shardId-003"));

        List<String> childShardId2 = shardCache.get("shardId-004");
        assertThat(childShardId2, notNullValue());
        assertThat(childShardId2.size(), equalTo(2));
        assertThat(childShardId2.contains("shardId-006"), equalTo(true));
        assertThat(childShardId2.contains("shardId-007"), equalTo(true));

        List<String> childShardId3 = shardCache.get("shardId-006");
        assertThat(childShardId3, nullValue());
    }

    private Map<String, String> createShardPairs() {
        // Key is child, value is parent
        Map<String, String> pairs = new HashMap<>();

        // 5 unique parents and 8 unique children
        pairs.put("shardId-001", null);
        pairs.put("shardId-002", null);
        pairs.put("shardId-003", "shardId-001");
        pairs.put("shardId-004", "shardId-002");
        pairs.put("shardId-005", "shardId-003");
        pairs.put("shardId-006", "shardId-004");
        pairs.put("shardId-007", "shardId-004");
        pairs.put("shardId-008", "shardId-007");
        return pairs;
    }
}