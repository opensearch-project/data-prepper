/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility for manage shards.
 */
public class ShardManager {

    private static final Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    private final DynamoDbStreamsClient streamsClient;

    public ShardManager(DynamoDbStreamsClient streamsClient) {
        this.streamsClient = streamsClient;
    }


    private List<Shard> listShards(String streamArn) {
        // Get all the shard IDs from the stream.
        List<Shard> shards;
        String lastEvaluatedShardId = null;
        do {
            DescribeStreamRequest req = DescribeStreamRequest.builder()
                    .streamArn(streamArn)
                    .exclusiveStartShardId(lastEvaluatedShardId)
                    .build();

            DescribeStreamResponse describeStreamResult = streamsClient.describeStream(req);
            shards = describeStreamResult.streamDescription().shards();

            // If LastEvaluatedShardId is set,
            // at least one more page of shard IDs to retrieve
            lastEvaluatedShardId = describeStreamResult.streamDescription().lastEvaluatedShardId();
        } while (lastEvaluatedShardId != null);

        LOG.debug("Stream {} has {} shards found", streamArn, shards.size());
        return shards;
    }

    /**
     * Get a list of Child Shard Ids based on a parent shard id provided.
     *
     * @param streamArn Stream Arn
     * @param shardId   Parent Shard Id
     * @return A list of child shard Ids.
     */
    public List<String> getChildShardIds(String streamArn, String shardId) {
        LOG.debug("Getting child ids for " + shardId);
        List<Shard> shards = listShards(streamArn);
        return shards.stream()
                .filter(s -> shardId.equals(s.parentShardId()))
                .map(s -> s.shardId())
                .collect(Collectors.toList());
    }

    /**
     * Get a list of active/open shards for a Stream.
     * They don't have an ending sequence number and is currently active for write.
     *
     * @param streamArn Stream Arn
     * @return A list of shard Ids
     */
    public List<String> getActiveShards(String streamArn) {
        List<Shard> shards = listShards(streamArn);
        return shards.stream()
                .filter(s -> s.sequenceNumberRange().endingSequenceNumber() == null)
                .map(s -> s.shardId())
                .collect(Collectors.toList());
    }


    /**
     * Get a shard iterator to start reading stream records from a shard.
     * If sequence number is provided, use AFTER_SEQUENCE_NUMBER to retrieve the iterator,
     * otherwise use TRIM_HORIZON to retrieve the iterator.
     * <p>
     * Note that the shard may be expired, if so, null will be returned.
     * </p>
     *
     * @param streamArn      Stream Arn
     * @param shardId        Shard Id
     * @param sequenceNumber The last Sequence Number processed if any
     * @return A shard iterator.
     */
    public String getShardIterator(String streamArn, String shardId, String sequenceNumber) {
        LOG.debug("Get Initial Shard Iter for " + shardId);
        GetShardIteratorRequest getShardIteratorRequest;

        if (sequenceNumber != null && !sequenceNumber.isEmpty()) {
            LOG.debug("Get Shard Iterator after " + sequenceNumber);
            getShardIteratorRequest = GetShardIteratorRequest.builder()
                    .shardId(shardId)
                    .streamArn(streamArn)
                    .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    .sequenceNumber(sequenceNumber)
                    .build();

        } else {
            LOG.debug("Get Shard Iterator from beginning (TRIM_HORIZON)");
            getShardIteratorRequest = GetShardIteratorRequest.builder()
                    .shardId(shardId)
                    .streamArn(streamArn)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build();
        }


        try {
            GetShardIteratorResponse getShardIteratorResult = streamsClient.getShardIterator(getShardIteratorRequest);
            String currentShardIter = getShardIteratorResult.shardIterator();
            return currentShardIter;
        } catch (SdkException e) {
            LOG.error("Exception when trying to get the shard iterator");
            LOG.error(e.getMessage());
            return null;
        }


    }

    /**
     * Get a list of root shard Ids.
     * A root shard is a shard whose parent shard is not in the list or whose parent id is null.
     *
     * @param streamArn Stream Arn
     * @return A list of root shard Ids
     */
    public List<String> getRootShardIds(String streamArn) {
        List<Shard> shards = listShards(streamArn);

        List<String> childIds = shards.stream().map(shard -> shard.shardId()).collect(Collectors.toList());
        List<String> rootIds = shards.stream()
                .filter(shard -> shard.parentShardId() == null || !childIds.contains(shard.parentShardId()))
                .map(shard -> shard.shardId())
                .collect(Collectors.toList());

        return rootIds;
    }

}
