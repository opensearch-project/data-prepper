/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Instant;
import java.util.Optional;

/**
 * Factory class to create shard consumers
 */
public class ShardConsumerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ShardManager.class);

    private static final int STREAM_TO_TABLE_OFFSET = "stream/".length();

    private final DynamoDbStreamsClient streamsClient;

    private final EnhancedSourceCoordinator enhancedSourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final ShardManager shardManager;

    private final Buffer<Record<Event>> buffer;

    public ShardConsumerFactory(final EnhancedSourceCoordinator enhancedSourceCoordinator,
                                final DynamoDbStreamsClient streamsClient, final PluginMetrics pluginMetrics,
                                final ShardManager shardManager, final Buffer<Record<Event>> buffer) {
        this.streamsClient = streamsClient;
        this.enhancedSourceCoordinator = enhancedSourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.shardManager = shardManager;
        this.buffer = buffer;

    }

    public Runnable createConsumer(StreamPartition streamPartition) {

        LOG.debug("Try to create a thread for shard " + streamPartition.getShardId());

        // Check and get the current state.
        Optional<StreamProgressState> progressState = streamPartition.getProgressState();
        String sequenceNumber = null;
        Instant startTime = null;
        boolean waitForExport = false;
        if (progressState.isPresent()) {
            sequenceNumber = progressState.get().getSequenceNumber();
            waitForExport = progressState.get().shouldWaitForExport();
            if (progressState.get().getStartTime() != 0) {
                startTime = Instant.ofEpochMilli(progressState.get().getStartTime());
            }
        }

        String shardIter = shardManager.getShardIterator(streamPartition.getStreamArn(), streamPartition.getShardId(), sequenceNumber);
        if (shardIter == null) {
            LOG.error("Unable to get a shard iterator, looks like the shard has expired");
            return null;
        }

        StreamCheckpointer checkpointer = new StreamCheckpointer(enhancedSourceCoordinator, streamPartition);
        String tableArn = getTableArn(streamPartition.getStreamArn());
        TableInfo tableInfo = getTableInfo(tableArn);
        StreamRecordConverter recordConverter = new StreamRecordConverter(buffer, tableInfo, pluginMetrics);

        ShardConsumer shardConsumer = ShardConsumer.builder(streamsClient)
                .recordConverter(recordConverter)
                .checkpointer(checkpointer)
                .shardIterator(shardIter)
                .startTime(startTime)
                .waitForExport(waitForExport)
                .build();
        return shardConsumer;
    }

    private TableInfo getTableInfo(String tableArn) {
        GlobalState tableState = (GlobalState) enhancedSourceCoordinator.getPartition(tableArn).get();
        TableInfo tableInfo = new TableInfo(tableArn, TableMetadata.fromMap(tableState.getProgressState().get()));
        return tableInfo;
    }

    private String getTableArn(String streamArn) {
        // e.g. Given a stream arn: arn:aws:dynamodb:us-west-2:xxx:table/test-table/stream/2023-07-31T04:59:58.190
        // Returns arn:aws:dynamodb:us-west-2:xxx:table/test-table
        return streamArn.substring(0, streamArn.lastIndexOf('/') - STREAM_TO_TABLE_OFFSET);
    }
}
