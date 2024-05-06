/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.export;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.BsonHelper;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.JSON_WRITER_SETTINGS;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.DOCUMENTDB_ID_FIELD_NAME;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.UNKNOWN_TYPE;

public class ExportPartitionWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportPartitionWorker.class);
    private static final int PARTITION_KEY_PARTS = 4;
    static final Duration VERSION_OVERLAP_TIME_FOR_EXPORT = Duration.ofMinutes(5);
    static final String SUCCESS_ITEM_COUNTER_NAME = "exportRecordsSuccessTotal";
    static final String FAILURE_ITEM_COUNTER_NAME = "exportRecordsFailedTotal";
    static final String BYTES_RECEIVED = "bytesReceived";
    private static final String PARTITION_KEY_SPLITTER = "\\|";
    private static final String COLLECTION_SPLITTER = "\\.";

    /**
     * Number of lines to be read in a batch
     */
    private static final int DEFAULT_BATCH_SIZE = 100;
    /**
     * Start Line is the checkpoint
     */
    private final int startLine;

    /**
     * Default regular checkpoint interval
     */
    private static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 2 * 60_000;
    private static final int DEFAULT_PARTITION_CREATE_WAIT_INTERVAL_MILLIS = 60_000;

    /**
     * A flag to interrupt the process
     */
    private static volatile boolean shouldStop = false;

    private final MongoDBSourceConfig sourceConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final RecordBufferWriter recordBufferWriter;
    private final PartitionKeyRecordConverter recordConverter;
    private final DataQueryPartition dataQueryPartition;
    private final AcknowledgementSet acknowledgementSet;
    private final long exportStartTime;
    private final DataQueryPartitionCheckpoint partitionCheckpoint;

    Optional<S3PartitionStatus> s3PartitionStatus = Optional.empty();

    public ExportPartitionWorker(final RecordBufferWriter recordBufferWriter,
                                 final PartitionKeyRecordConverter recordConverter,
                                 final DataQueryPartition dataQueryPartition,
                                 final AcknowledgementSet acknowledgementSet,
                                 final MongoDBSourceConfig sourceConfig,
                                 final DataQueryPartitionCheckpoint partitionCheckpoint,
                                 final long exportStartTime,
                                 final PluginMetrics pluginMetrics) {
        this.recordBufferWriter = recordBufferWriter;
        this.recordConverter = recordConverter;
        this.dataQueryPartition = dataQueryPartition;
        this.acknowledgementSet = acknowledgementSet;
        this.sourceConfig = sourceConfig;
        this.partitionCheckpoint = partitionCheckpoint;
        this.startLine = 0;// replace it with checkpoint line
        this.exportStartTime = exportStartTime;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
    }

    private boolean shouldWaitForS3Partition(final String collection) {
        s3PartitionStatus = partitionCheckpoint.getGlobalS3FolderCreationStatus(collection);
        return s3PartitionStatus.isEmpty();
    }

    @Override
    public void run() {
        final List<String> partitionKeys = List.of(dataQueryPartition.getPartitionKey().split(PARTITION_KEY_SPLITTER));
        if (partitionKeys.size() < PARTITION_KEY_PARTS) {
            throw new RuntimeException("Invalid Partition Key. Must as db.collection|gte|lte format. Key: " + dataQueryPartition.getPartitionKey());
        }
        final List<String> collection = List.of(partitionKeys.get(0).split(COLLECTION_SPLITTER));
        final String gte = partitionKeys.get(1);
        final String lte = partitionKeys.get(2);
        final String gteClassName = partitionKeys.get(3);
        final String lteClassName = partitionKeys.get(4);
        if (collection.size() < 2) {
            throw new RuntimeException("Invalid Collection Name. Must as db.collection format");
        }
        long lastCheckpointTime = System.currentTimeMillis();
        while (shouldWaitForS3Partition(dataQueryPartition.getCollection()) && !Thread.currentThread().isInterrupted()) {
            LOG.info("S3 partition was not complete for collection {}, waiting for partitions to be created before resuming export.", dataQueryPartition.getCollection());
            try {
                Thread.sleep(DEFAULT_PARTITION_CREATE_WAIT_INTERVAL_MILLIS);
            } catch (final InterruptedException ex) {
                LOG.info("The ExportPartitionWorker was interrupted while waiting to retry, stopping the worker");
                Thread.currentThread().interrupt();
                break;
            }
        }
        final List<String> s3Partitions = s3PartitionStatus.get().getPartitions();
        if (s3Partitions.isEmpty()) {
            // This should not happen unless the S3 partition creator failed.
            throw new IllegalStateException("S3 partitions are not created. Please check the S3 partition creator thread.");
        }

        recordConverter.initializePartitions(s3Partitions);
        try (final MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            final MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            final MongoCollection<Document> col = db.getCollection(partitionKeys.get(0).substring(collection.get(0).length()+1));
            final Bson query = BsonHelper.buildQuery(gte, lte, gteClassName, lteClassName);
            long totalRecords = 0L;
            long successRecords = 0L;
            long failedRecords = 0L;

            // line count regardless the start line number
            int recordCount = 0;
            int lastRecordNumberProcessed = 0;
            final List<Event> records = new ArrayList<>();
            try (MongoCursor<Document> cursor = col.find(query).iterator()) {
                while (cursor.hasNext() && !Thread.currentThread().isInterrupted()) {
                    if (shouldStop) {
                        partitionCheckpoint.checkpoint(lastRecordNumberProcessed);
                        LOG.warn("Loading data query {} was interrupted by a shutdown signal, giving up ownership of " +
                                "query partition", query);
                        throw new RuntimeException("Loading data query interrupted");
                    }
                    totalRecords += 1;
                    recordCount += 1;
                    if (totalRecords <= startLine) {
                        continue;
                    }

                    try {
                        final Document document = cursor.next();
                        final String record = document.toJson(JSON_WRITER_SETTINGS);
                        final long bytes = record.getBytes().length;
                        bytesReceivedSummary.record(bytes);
                        final Optional<BsonDocument> primaryKeyDoc = Optional.ofNullable(document.toBsonDocument());
                        final String primaryKeyBsonType = primaryKeyDoc.map(bsonDocument -> bsonDocument.getBsonType().name()).orElse(UNKNOWN_TYPE);

                        // The version number is the export time minus some overlap to ensure new stream events still get priority
                        final long eventVersionNumber = (exportStartTime - VERSION_OVERLAP_TIME_FOR_EXPORT.toMillis()) * 1_000;
                        final Event event = recordConverter.convert(record, exportStartTime, eventVersionNumber, primaryKeyBsonType);
                        // event.put(DEFAULT_ID_MAPPING_FIELD_NAME, event.get(DOCUMENTDB_ID_FIELD_NAME, Object.class));
                        // delete _id
                        event.delete(DOCUMENTDB_ID_FIELD_NAME);
                        records.add(event);

                        if ((recordCount - startLine) % DEFAULT_BATCH_SIZE == 0) {
                            LOG.debug("Write to buffer for line " + (recordCount - DEFAULT_BATCH_SIZE) + " to " + recordCount);
                            recordBufferWriter.writeToBuffer(acknowledgementSet, records);
                            records.clear();
                            lastRecordNumberProcessed = recordCount;
                            // checkpointing in finally block when all records are processed
                        }

                        if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                            LOG.debug("Perform regular checkpointing for Data Query Loader");
                            partitionCheckpoint.checkpoint(lastRecordNumberProcessed);
                            lastCheckpointTime = System.currentTimeMillis();
                        }

                        successItemsCounter.increment();
                        successRecords += 1;
                    } catch (Exception e) {
                        LOG.error("Failed to add record to buffer with error {}", e.getMessage());
                        failureItemsCounter.increment();
                        failedRecords += 1;
                    }
                }

                if (!records.isEmpty()) {
                    // Do final checkpoint.
                    // If all records were written to buffer, checkpoint will be done in finally block
                    recordBufferWriter.writeToBuffer(acknowledgementSet, records);
                    partitionCheckpoint.checkpoint(recordCount);
                }

                records.clear();

                LOG.info("Completed writing query partition: {} to buffer", query);

                if (acknowledgementSet != null) {
                    partitionCheckpoint.updateDatafileForAcknowledgmentWait(sourceConfig.getPartitionAcknowledgmentTimeout());
                    acknowledgementSet.complete();
                }

            } catch (Exception e) {
                LOG.error("Exception connecting to cluster and loading partition {}. Exception: {}", query, e.getMessage());
                throw new RuntimeException(e);
            } finally {
                // Do final checkpoint when reaching end of partition or due to exception
                partitionCheckpoint.checkpoint(recordCount);
            }

            LOG.info("Records processed: {}, recordCount: {}", totalRecords, recordCount);
        }
    }

    /**
     * Currently, this is to stop all consumers.
     */
    public static void stopAll() {
        shouldStop = true;
    }
}
