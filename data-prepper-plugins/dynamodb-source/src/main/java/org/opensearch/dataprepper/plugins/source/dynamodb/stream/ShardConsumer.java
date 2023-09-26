/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.stream;


import org.opensearch.dataprepper.plugins.source.dynamodb.converter.StreamRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A basic data consumer to read from one shard
 */
public class ShardConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ShardConsumer.class);

    // A flag to interrupt the process
    private static volatile boolean shouldStop = false;

    private static final int MAX_GET_RECORD_ITEM_COUNT = 1000;

    // Idle Time between Reads
    private static final int GET_RECORD_INTERVAL_MILLS = 200;

    private static final int DEFAULT_WAIT_FOR_EXPORT_INTERVAL_MILLS = 60_000;

    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 2 * 60_000;

    private final DynamoDbStreamsClient dynamoDbStreamsClient;

    private final StreamRecordConverter recordConverter;

    private final StreamCheckpointer checkpointer;

    private String shardIterator;

    private final Instant startTime;

    private boolean waitForExport;

    private ShardConsumer(Builder builder) {
        this.dynamoDbStreamsClient = builder.dynamoDbStreamsClient;
        this.recordConverter = builder.recordConverter;
        this.checkpointer = builder.checkpointer;
        this.shardIterator = builder.shardIterator;
        this.startTime = builder.startTime;
        this.waitForExport = builder.waitForExport;
    }

    public static Builder builder(DynamoDbStreamsClient streamsClient) {
        return new Builder(streamsClient);
    }


    static class Builder {

        private final DynamoDbStreamsClient dynamoDbStreamsClient;


        private StreamRecordConverter recordConverter;

        private StreamCheckpointer checkpointer;

        private String shardIterator;


        private Instant startTime;

        private boolean waitForExport;

        public Builder(DynamoDbStreamsClient dynamoDbStreamsClient) {
            this.dynamoDbStreamsClient = dynamoDbStreamsClient;
        }

        public Builder recordConverter(StreamRecordConverter recordConverter) {
            this.recordConverter = recordConverter;
            return this;
        }

        public Builder checkpointer(StreamCheckpointer checkpointer) {
            this.checkpointer = checkpointer;
            return this;
        }

        public Builder shardIterator(String shardIterator) {
            this.shardIterator = shardIterator;
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

        long lastCheckpointTime = System.currentTimeMillis();
        String sequenceNumber = "";

        while (!shouldStop) {
            if (shardIterator == null) {
                // End of Shard
                LOG.debug("Reach end of shard");
                checkpointer.checkpoint(sequenceNumber);
                break;
            }

            if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                LOG.debug("Perform regular checkpointing for Shard Consumer");
                checkpointer.checkpoint(sequenceNumber);
                lastCheckpointTime = System.currentTimeMillis();
            }

            // Use the shard iterator to read the stream records
            GetRecordsRequest req = GetRecordsRequest.builder()
                    .shardIterator(shardIterator)
                    .limit(MAX_GET_RECORD_ITEM_COUNT)
                    .build();


            List<Record> records;
            GetRecordsResponse response;
            try {
                response = dynamoDbStreamsClient.getRecords(req);
            } catch (SdkException e) {
                checkpointer.checkpoint(sequenceNumber);
                throw e;
            }

            shardIterator = response.nextShardIterator();

            if (!response.records().isEmpty()) {
                // Always use the last sequence number for checkpoint
                sequenceNumber = response.records().get(response.records().size() - 1).dynamodb().sequenceNumber();

                if (waitForExport) {
                    Instant lastEventTime = response.records().get(response.records().size() - 1).dynamodb().approximateCreationDateTime();
                    if (lastEventTime.compareTo(startTime) <= 0) {
                        LOG.debug("Get {} events before start time, ignore...", response.records().size());
                        continue;
                    }
                    checkpointer.checkpoint(sequenceNumber);
                    waitForExport();
                    waitForExport = false;

                    records = response.records().stream()
                            .filter(record -> record.dynamodb().approximateCreationDateTime().compareTo(startTime) > 0)
                            .collect(Collectors.toList());
                } else {
                    records = response.records();
                }
                recordConverter.writeToBuffer(records);
            }
            try {
                // Idle between get records call.
                Thread.sleep(GET_RECORD_INTERVAL_MILLS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // interrupted
        if (shouldStop) {
            // Do last checkpoint and then quit
            LOG.error("Should Stop flag is set to True, looks like shutdown has triggered");
            checkpointer.checkpoint(sequenceNumber);
            throw new RuntimeException("Shard Consumer is interrupted");
        }
    }

    private void waitForExport() {
        LOG.debug("Start waiting for export to be done and loaded");
        while (!checkpointer.isExportDone()) {
            LOG.debug("Export is in progress, wait...");
            try {
                Thread.sleep(DEFAULT_WAIT_FOR_EXPORT_INTERVAL_MILLS);
            } catch (InterruptedException e) {
                LOG.error("Wait for export is interrupted ({})", e.getMessage());
                // Directly quit the process
                throw new RuntimeException("Wait for export is interrupted.");
            }
        }
    }


    /**
     * Currently, this is to stop all consumers.
     */
    public static void stopAll() {
        shouldStop = true;
    }


}