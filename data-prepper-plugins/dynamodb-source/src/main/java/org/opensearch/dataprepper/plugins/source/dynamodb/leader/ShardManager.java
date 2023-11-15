package org.opensearch.dataprepper.plugins.source.dynamodb.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A general manager class to handle shard related task
 */
public class ShardManager {

    private static final Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    /**
     * Max number of shards to return in the DescribeStream API call, maximum 100.
     */
    private static final int MAX_SHARD_COUNT = 100;

    /**
     * Default interval to clean up cache and rebuild
     */
    private static final int DEFAULT_CLEAN_UP_CACHE_INTERVAL_MILLS = 10 * 60_000;

    /**
     * A map for all streams, where key is streamArn, and the value is the related {@link StreamInfo}.
     */
    private final Map<String, StreamInfo> streamMap;

    /**
     * A map for storing ending sequence number, where key is shard ID and the value is the ending sequence number.
     */
    private Map<String, String> endingSequenceNumberMap;


    private final DynamoDbStreamsClient streamsClient;


    public ShardManager(final DynamoDbStreamsClient streamsClient) {
        this.streamsClient = streamsClient;
        streamMap = new HashMap<>();
        endingSequenceNumberMap = new HashMap<>();
    }

    /**
     * <p>This is the main process for shard discovery (listing shards using DescribeStream API).
     * It will use the last evaluated shard ID to speed up the listing,
     * but still run a full listing on a regular basis.</p>
     *
     * <p>Everytime the process run, it also builds the internal caching store,
     * which will be used to find child shards for a given parent.</p>
     *
     * @param streamArn Stream ARN
     * @return a list of {@link Shard}
     */
    public List<Shard> runDiscovery(String streamArn) {
        StreamInfo streamInfo = streamMap.get(streamArn);

        if (streamInfo == null) {
            streamInfo = new StreamInfo();
            streamInfo.setLastCacheBuildTime(System.currentTimeMillis());
            streamInfo.setLastEvaluatedShardId(null);
            streamInfo.setShardCache(new ShardCache());
            streamMap.put(streamArn, streamInfo);
        }

        ShardCache shardCache = streamInfo.getShardCache();
        if (System.currentTimeMillis() - streamInfo.getLastCacheBuildTime() > DEFAULT_CLEAN_UP_CACHE_INTERVAL_MILLS) {
            LOG.debug("Perform regular rebuild of cache.");
            // Reset the mask
            streamInfo.setLastEvaluatedShardId(null);
            streamInfo.setLastCacheBuildTime(System.currentTimeMillis());
            // Clean up existing cache.
            shardCache.clear();
            endingSequenceNumberMap.clear();
        }


        LOG.debug("Last evaluated shard ID is " + streamInfo.getLastEvaluatedShardId());
        List<Shard> shards = listShards(streamArn, streamInfo.getLastEvaluatedShardId());
        // build/update cache
        if (!shards.isEmpty()) {
            shards.forEach(shard -> {
                shardCache.put(shard.shardId(), shard.parentShardId());
            });

            if (streamInfo.getLastEvaluatedShardId() == null) {
                endingSequenceNumberMap = shards.stream()
                        .filter(shard -> shard.sequenceNumberRange().endingSequenceNumber() != null)
                        .collect(Collectors.toMap(
                                shard -> shard.shardId(),
                                shard -> shard.sequenceNumberRange().endingSequenceNumber()
                        ));
            }
            LOG.debug("New last evaluated shard ID is " + shards.get(shards.size() - 1).shardId());
            streamInfo.setLastEvaluatedShardId(shards.get(shards.size() - 1).shardId());
        }
        return shards;
    }

    /**
     * Ending sequence number is used when trying to create a closed shard.
     *
     * @param shardId Shard ID
     * @return the related ending sequence number if any, otherwise return null;
     */
    public String getEndingSequenceNumber(String shardId) {
        // May change this if multiple tables are supported.
        return endingSequenceNumberMap.get(shardId);
    }

    /**
     * Finding child shards from cache
     *
     * @param streamArn     Stream ARN
     * @param parentShardId Parent Shard IDs
     * @return a list of shard IDs
     */
    public List<String> findChildShardIds(String streamArn, String parentShardId) {
        StreamInfo streamInfo = streamMap.get(streamArn);
        if (streamInfo == null) {
            return Collections.emptyList();
        }

        ShardCache shardCache = streamInfo.getShardCache();
        List<String> childShardIds = shardCache.get(parentShardId);
        return childShardIds;
    }

    /**
     * List all shards using DescribeStream API.
     *
     * @param streamArn            Stream Arn
     * @param lastEvaluatedShardId Start shard id for listing, useful when trying to get child shards. If not provided, all shards will be returned.
     * @return A list of {@link Shard}
     */
    private List<Shard> listShards(String streamArn, String lastEvaluatedShardId) {
        LOG.debug("Start listing all shards for stream {}", streamArn);
        long startTime = System.currentTimeMillis();
        // Get all the shard IDs from the stream.
        List<Shard> shards = new ArrayList<>();
        do {
            DescribeStreamRequest req = DescribeStreamRequest.builder()
                    .streamArn(streamArn)
                    .limit(MAX_SHARD_COUNT)
                    .exclusiveStartShardId(lastEvaluatedShardId)
                    .build();

            DescribeStreamResponse describeStreamResult = streamsClient.describeStream(req);
            shards.addAll(describeStreamResult.streamDescription().shards());

            // If LastEvaluatedShardId is set,
            // at least one more page of shard IDs to retrieve
            lastEvaluatedShardId = describeStreamResult.streamDescription().lastEvaluatedShardId();


        } while (lastEvaluatedShardId != null);

        long endTime = System.currentTimeMillis();
        LOG.info("Listing shards (DescribeStream call) took {} milliseconds with {} shards found", endTime - startTime, shards.size());
        return shards;
    }


    /**
     * Extra state for shard discovery for each stream
     */
    class StreamInfo {
        private String lastEvaluatedShardId;
        private long lastCacheBuildTime;
        private ShardCache shardCache;

        public String getLastEvaluatedShardId() {
            return lastEvaluatedShardId;
        }

        public void setLastEvaluatedShardId(String lastEvaluatedShardId) {
            this.lastEvaluatedShardId = lastEvaluatedShardId;
        }

        public long getLastCacheBuildTime() {
            return lastCacheBuildTime;
        }

        public void setLastCacheBuildTime(long lastCacheBuildTime) {
            this.lastCacheBuildTime = lastCacheBuildTime;
        }

        public ShardCache getShardCache() {
            return shardCache;
        }

        public void setShardCache(ShardCache shardCache) {
            this.shardCache = shardCache;
        }
    }


}
