package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A Caching store for quickly finding child shards by a given parent shard ID.
 * Should create one for each stream in case of conflicts of shard Ids
 */
public class ShardCache {

    /**
     * Common prefix for shard ID
     */
    private static final String DEFAULT_SHARD_ID_PREFIX = "shardId-";

    /**
     * A cache in HashMap, where key is parent shard id, and the value is the list of child shard Ids.
     */
    private final Map<String, List<String>> cache;

    public ShardCache() {
        this.cache = new HashMap<>();
    }

    /**
     * Add a parent-child shard pair to cache.
     *
     * @param shardId       Shard ID
     * @param parentShardId Parent Shard ID
     */
    public void put(final String shardId, final String parentShardId) {
        Objects.requireNonNull(shardId);
        if (parentShardId != null && !parentShardId.isEmpty()) {
            String trimedParentShardId = removeShardIdPrefix(parentShardId);
            String trimedShardId = removeShardIdPrefix(shardId);
            List<String> childShards = cache.getOrDefault(trimedParentShardId, new ArrayList<>());
            childShards.add(trimedShardId);
            cache.put(trimedParentShardId, childShards);
        }
    }


    /**
     * Get child shard ids by parent shard id from cache.
     * If none is found, return null.
     *
     * @param parentShardId
     * @return a list of Child Shard IDs
     */
    public List<String> get(String parentShardId) {
        List<String> childShardIds = cache.get(removeShardIdPrefix(parentShardId));
        if (childShardIds == null) {
            return null;
        }
        return childShardIds.stream().map(this::appendShardIdPrefix).collect(Collectors.toList());
    }

    /**
     * Clean up cache
     */
    public void clear() {
        cache.clear();
    }


    /**
     * Get cache size
     *
     * @return size of the map
     */
    public int size() {
        return cache.size();
    }


    /**
     * Remove the common prefix to save space
     */
    private String removeShardIdPrefix(String shardId) {
        return shardId.substring(DEFAULT_SHARD_ID_PREFIX.length());
    }

    /**
     * Append the common prefix back when retrieval
     */
    private String appendShardIdPrefix(String shardId) {
        return DEFAULT_SHARD_ID_PREFIX + shardId;
    }


}
