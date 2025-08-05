/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.opensearch.dataprepper.plugins.source.dynamodb.utils.DynamoDBSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A basic data consumer to read from one shard
 */
public class ShardConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    /**
     * A flag to interrupt the process
     */
    private static volatile boolean shouldStop = false;

    /**
     * An overlap added between the event creation time and the export time
     */
    private static final Duration STREAM_EVENT_OVERLAP_TIME = Duration.ofMinutes(5);

    /**
     * Max number of items to return per GetRecords call, maximum 1000.
     */
    private static final int MAX_GET_RECORD_ITEM_COUNT = 1000;

    /**
     * Idle Time between GetRecords Reads
     */
    private static final int GET_RECORD_INTERVAL_MILLS = 300;

    /**
     * Idle Time between GetRecords Reads
     */
    private static final int MINIMUM_GET_RECORD_INTERVAL_MILLS = 10;

    /**
     * Minimum Idle Time between GetRecords Reads
     */
    private static final long GET_RECORD_DELAY_THRESHOLD_MILLS = 15_000;

    /**
     * Default interval to check if export is completed.
     */
    private static final int DEFAULT_WAIT_FOR_EXPORT_INTERVAL_MILLS = 60_000;


    /**
     * Default number of times in the wait for export to do regular checkpoint.
     */
    private static final int DEFAULT_WAIT_COUNT_TO_CHECKPOINT = 3;

    /**
     * Default regular checkpoint interval
     */
    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 2 * 60_000;

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String SHARD_PROGRESS = "shardProgress";


    private final DynamoDbStreamsClient dynamoDbStreamsClient;

    private final StreamRecordConverter recordConverter;

    private final StreamCheckpointer checkpointer;

    private final ShardAcknowledgementManager shardAcknowledgementManager;

    private final StreamPartition streamPartition;

    private String shardIterator;

    private final String lastShardIterator;

    private final Instant startTime;

    private boolean waitForExport;

    private final String shardId;

    private final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics;

    private final Counter shardProgress;

    private long recordsWrittenToBuffer;

    private ShardConsumer(Builder builder) {
        this.shardProgress = builder.pluginMetrics.counter(SHARD_PROGRESS);
        this.dynamoDbStreamsClient = builder.dynamoDbStreamsClient;
        this.checkpointer = builder.checkpointer;
        this.shardAcknowledgementManager = builder.shardAcknowledgementManager;
        this.streamPartition = builder.streamPartition;
        this.shardIterator = builder.shardIterator;
        this.lastShardIterator = builder.lastShardIterator;
        // Introduce an overlap
        this.startTime = builder.startTime == null ? Instant.MIN : builder.startTime.minus(STREAM_EVENT_OVERLAP_TIME);
        this.waitForExport = builder.waitForExport;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(builder.buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        recordConverter = new StreamRecordConverter(bufferAccumulator, builder.tableInfo, builder.pluginMetrics, builder.streamConfig);
        this.shardId = builder.shardId;
        this.recordsWrittenToBuffer = 0;
        this.dynamoDBSourceAggregateMetrics = builder.dynamoDBSourceAggregateMetrics;
    }

    public static Builder builder(final DynamoDbStreamsClient dynamoDbStreamsClient,
                                  final PluginMetrics pluginMetrics,
                                  final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics,
                                  final Buffer<Record<Event>> buffer,
                                  final StreamConfig streamConfig) {
        return new Builder(dynamoDbStreamsClient, pluginMetrics, dynamoDBSourceAggregateMetrics, buffer, streamConfig);
    }


    static class Builder {

        private final DynamoDbStreamsClient dynamoDbStreamsClient;

        private final PluginMetrics pluginMetrics;

        private final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics;

        private final Buffer<Record<Event>> buffer;

        private TableInfo tableInfo;

        private StreamCheckpointer checkpointer;

        private ShardAcknowledgementManager shardAcknowledgementManager;

        private StreamPartition streamPartition;

        private String shardIterator;

        private String lastShardIterator;

        private Instant startTime;

        private boolean waitForExport;

        private String shardId;

        private StreamConfig streamConfig;

        public Builder(final DynamoDbStreamsClient dynamoDbStreamsClient,
                       final PluginMetrics pluginMetrics,
                       final DynamoDBSourceAggregateMetrics dynamoDBSourceAggregateMetrics,
                       final Buffer<Record<Event>> buffer,
                       final StreamConfig streamConfig) {
            this.dynamoDbStreamsClient = dynamoDbStreamsClient;
            this.pluginMetrics = pluginMetrics;
            this.dynamoDBSourceAggregateMetrics = dynamoDBSourceAggregateMetrics;
            this.buffer = buffer;
            this.streamConfig = streamConfig;
        }

        public Builder tableInfo(TableInfo tableInfo) {
            this.tableInfo = tableInfo;
            return this;
        }

        public Builder shardId(final String shardId) {
            this.shardId = shardId;
            return this;
        }

        public Builder checkpointer(StreamCheckpointer checkpointer) {
            this.checkpointer = checkpointer;
            return this;
        }

        public Builder shardAcknowledgementManager(ShardAcknowledgementManager shardAcknowledgementManager) {
            this.shardAcknowledgementManager = shardAcknowledgementManager;
            return this;
        }

        public Builder streamPartition(StreamPartition streamPartition) {
            this.streamPartition = streamPartition;
            return this;
        }

        public Builder shardIterator(String shardIterator) {
            this.shardIterator = shardIterator;
            return this;
        }

        public Builder lastShardIterator(String lastShardIterator) {
            this.lastShardIterator = lastShardIterator;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder waitForExport(boolean waitForExport) {
            this.waitForExport = waitForExport;
            return this;
        }

        public ShardConsumer build() {
            return new ShardConsumer(this);
        }

    }


    @Override
    public void run() {
        LOG.debug("Shard Consumer start to run...");
        // Check should skip processing or not.
        if (shouldSkip()) {
            shardProgress.increment();
            if (shardAcknowledgementManager != null) {
                checkpointer.completePartition();
            }
            return;
        }
        if (shardAcknowledgementManager != null) {
            shardAcknowledgementManager.startUpdatingOwnershipForShard(streamPartition);
        }
        long lastCheckpointTime = System.currentTimeMillis();
        String sequenceNumber = "";
        int interval;
        List<software.amazon.awssdk.services.dynamodb.model.Record> records;

        while (!shouldStop) {
            if (shardIterator == null) {
                // End of Shard
                LOG.debug("Reached end of shard");
                break;
            }

            if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                LOG.debug("{} records written to buffer for shard {}", recordsWrittenToBuffer, shardId);
                if (shardAcknowledgementManager == null) {
                    checkpointer.checkpoint(sequenceNumber);
                }
                lastCheckpointTime = System.currentTimeMillis();
            }

            GetRecordsResponse response = callGetRecords(shardIterator);
            shardIterator = response.nextShardIterator();
            if (!response.records().isEmpty()) {
                // Always use the last sequence number for checkpoint
                sequenceNumber = response.records().get(response.records().size() - 1).dynamodb().sequenceNumber();
                Instant lastEventTime = response.records().get(response.records().size() - 1).dynamodb().approximateCreationDateTime();

                if (lastEventTime.isBefore(startTime)) {
                    LOG.debug("Get {} events before start time, ignore...", response.records().size());
                    continue;
                }
                if (waitForExport) {
                    waitForExport();
                    waitForExport = false;
                }

                AcknowledgementSet acknowledgementSet = null;
                if (shardAcknowledgementManager != null) {
                    acknowledgementSet = shardAcknowledgementManager.createAcknowledgmentSet(streamPartition, sequenceNumber, shardIterator == null);
                }

                records = response.records().stream()
                        .filter(record -> record.dynamodb().approximateCreationDateTime().isAfter(startTime))
                        .collect(Collectors.toList());

                recordConverter.writeToBuffer(acknowledgementSet, records);
                if (acknowledgementSet != null) {
                    acknowledgementSet.complete();
                }

                shardProgress.increment();
                recordsWrittenToBuffer += records.size();
                long delay = System.currentTimeMillis() - lastEventTime.toEpochMilli();
                interval = delay > GET_RECORD_DELAY_THRESHOLD_MILLS ? MINIMUM_GET_RECORD_INTERVAL_MILLS : GET_RECORD_INTERVAL_MILLS;

            } else {
                interval = GET_RECORD_INTERVAL_MILLS;
                shardProgress.increment();
            }

            try {
                // Idle between get records call.
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // interrupted
        if (shouldStop) {
            // Do last checkpoint and then quit
            LOG.warn("Processing for shard {} was interrupted by a shutdown signal, giving up shard", shardId);
            throw new RuntimeException("Consuming shard was interrupted from shutdown");
        }

        if (waitForExport) {
            waitForExport();
        }
    }

    /**
     * Wrap of GetRecords call
     */
    private GetRecordsResponse callGetRecords(String shardIterator) {
        // Use the shard iterator to read the stream records
        GetRecordsRequest req = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(MAX_GET_RECORD_ITEM_COUNT)
                .build();

        try {
            dynamoDBSourceAggregateMetrics.getStreamApiInvocations().increment();
            GetRecordsResponse response = dynamoDbStreamsClient.getRecords(req);
            return response;
        } catch(final InternalServerErrorException ex) {
            dynamoDBSourceAggregateMetrics.getStream5xxErrors().increment();
            throw new RuntimeException(ex.getMessage());
        } catch (final Exception e) {
            dynamoDBSourceAggregateMetrics.getStream4xxErrors().increment();
            throw new RuntimeException(e.getMessage());
        }

    }

    private void waitForExport() {
        LOG.debug("Start waiting for export to be done and loaded");
        int numberOfWaits = 0;
        while (!checkpointer.isExportDone()) {
            LOG.debug("Export is in progress, wait...");
            try {
                shardProgress.increment();
                Thread.sleep(DEFAULT_WAIT_FOR_EXPORT_INTERVAL_MILLS);
                // The wait for export may take a long time
                // Need to extend the timeout of the ownership in the coordination store.
                // Otherwise, the lease will expire.
                numberOfWaits++;
                if (numberOfWaits % DEFAULT_WAIT_COUNT_TO_CHECKPOINT == 0) {
                    // To extend the timeout of lease
                    if (shardAcknowledgementManager == null) {
                        checkpointer.checkpoint(null);
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Wait for export is interrupted ({})", e.getMessage());
                // Directly quit the process
                throw new RuntimeException("Wait for export is interrupted.");
            }
        }
    }

    /**
     * Only to skip processing when below two conditions are met.
     * - Last Shard Iterator is provided (Shard with ending sequence number)
     * - Last Event Timestamp is later than start time or No Last Event Timestamp (empty shard)
     */
    private boolean shouldSkip() {
        // Do skip check
        if (lastShardIterator != null && !lastShardIterator.isEmpty()) {
            GetRecordsResponse response = callGetRecords(lastShardIterator);
            if (response.records().isEmpty()) {
                // Empty shard
                LOG.info("LastShardIterator is provided, but there is no Last Event Time, skip processing");
                return true;
            }

            Instant lastEventTime = response.records().get(response.records().size() - 1).dynamodb().approximateCreationDateTime();
            if (lastEventTime.isBefore(startTime)) {
                LOG.info("LastShardIterator is provided, and Last Event Time is earlier than {}, skip processing", startTime);
                return true;
            } else {
                LOG.info("LastShardIterator is provided, and Last Event Time is later than {}, start processing", startTime);
                return false;
            }
        }

        return false;
    }


    /**
     * Currently, this is to stop all consumers.
     */
    public static void stopAll() {
        shouldStop = true;
    }

}