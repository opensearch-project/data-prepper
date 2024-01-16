/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.DynamoDBSourceAggregateMetrics;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Factory class to create shard consumers
 */
public class ShardConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumerFactory.class);


    private final DynamoDbStreamsClient streamsClient;

    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics;
    private final Buffer<Record<Event>> buffer;


    public ShardConsumerFactory(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                final DynamoDbStreamsClient streamsClient,
                                final PluginMetrics pluginMetrics,
                                final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics,
                                final Buffer<Record<Event>> buffer) {
        this.streamsClient = streamsClient;
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.dynamoDBSourceAggregateMetrics = dynamoDBSourceAggregateMetrics;
        this.buffer = buffer;

    }

    public Runnable createConsumer(final StreamPartition streamPartition,
                                   final AcknowledgementSet acknowledgementSet,
                                   final Duration shardAcknowledgmentTimeout) {

        LOG.info("Starting to consume shard " + streamPartition.getShardId());

        // Check and get the current state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        String sequenceNumber = null;
        String lastShardIterator = null;
        Instant startTime = null;
        boolean waitForExport = false;
        if (progressState.isPresent()) {
            // We can't checkpoint with acks yet
            sequenceNumber = acknowledgementSet == null ? null : progressState.get().getSequenceNumber();
            waitForExport = progressState.get().shouldWaitForExport();
            if (progressState.get().getStartTime() != 0) {
                startTime = Instant.ofEpochMilli(progressState.get().getStartTime());
            }
            // If ending sequence number is present, get the shardIterator for last record
            String endingSequenceNumber = progressState.get().getEndingSequenceNumber();
            if (endingSequenceNumber != null && !endingSequenceNumber.isEmpty()) {
                lastShardIterator = getShardIterator(streamPartition.getStreamArn(), streamPartition.getShardId(), endingSequenceNumber);
            }
        }

        String shardIterator = getShardIterator(streamPartition.getStreamArn(), streamPartition.getShardId(), sequenceNumber);
        if (shardIterator == null) {
            LOG.error("Failed to start consuming shard '{}'. Unable to get a shard iterator for this shard, this shard may have expired", streamPartition.getShardId());
            return null;
        }

        StreamCheckpointer checkpointer = new StreamCheckpointer(enhancedSourceCoordinator, streamPartition);
        String tableArn = TableUtil.getTableArnFromStreamArn(streamPartition.getStreamArn());
        TableInfo tableInfo = getTableInfo(tableArn);

        LOG.debug("Create shard consumer for {} with shardIter {}", streamPartition.getShardId(), shardIterator);
        LOG.debug("Create shard consumer for {} with lastShardIter {}", streamPartition.getShardId(), lastShardIterator);
        ShardConsumer shardConsumer = ShardConsumer.builder(streamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer)
                .tableInfo(tableInfo)
                .checkpointer(checkpointer)
                .shardIterator(shardIterator)
                .shardId(streamPartition.getShardId())
                .lastShardIterator(lastShardIterator)
                .startTime(startTime)
                .waitForExport(waitForExport)
                .acknowledgmentSet(acknowledgementSet)
                .acknowledgmentSetTimeout(shardAcknowledgmentTimeout)
                .build();
        return shardConsumer;
    }

    private TableInfo getTableInfo(String tableArn) {
        GlobalState tableState = (GlobalState) enhancedSourceCoordinator.getPartition(tableArn).get();
        TableInfo tableInfo = new TableInfo(tableArn, TableMetadata.fromMap(tableState.getProgressState().get()));
        return tableInfo;
    }


    /**
     * Get a shard iterator to start reading stream records from a shard.
     * If sequence number is provided, use AT_SEQUENCE_NUMBER to retrieve the iterator,
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
        LOG.debug("Get Initial Shard Iter for {}", shardId);
        GetShardIteratorRequest getShardIteratorRequest;

        if (sequenceNumber != null && !sequenceNumber.isEmpty()) {
            LOG.debug("Get Shard Iterator at {}", sequenceNumber);
            // There may be an overlap for 1 record
            getShardIteratorRequest = GetShardIteratorRequest.builder()
                    .shardId(shardId)
                    .streamArn(streamArn)
                    .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                    .sequenceNumber(sequenceNumber)
                    .build();
        } else {
            LOG.debug("Get Shard Iterator from beginning (TRIM_HORIZON) for shard {}", shardId);
            getShardIteratorRequest = GetShardIteratorRequest.builder()
                    .shardId(shardId)
                    .streamArn(streamArn)
                    .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                    .build();
        }

        try {
            dynamoDBSourceAggregateMetrics.getStreamApiInvocations().increment();
            GetShardIteratorResponse getShardIteratorResult = streamsClient.getShardIterator(getShardIteratorRequest);
            return getShardIteratorResult.shardIterator();
        } catch (final InternalServerErrorException e) {
            dynamoDBSourceAggregateMetrics.getStream5xxErrors().increment();
            LOG.error("Received an internal server error from DynamoDB while getting a shard iterator: {}", e.getMessage());
            return null;
        } catch (SdkException e) {
            dynamoDBSourceAggregateMetrics.getStream4xxErrors().increment();
            LOG.error("Exception when trying to get the shard iterator due to {}", e.getMessage());
            return null;
        }
    }
}
