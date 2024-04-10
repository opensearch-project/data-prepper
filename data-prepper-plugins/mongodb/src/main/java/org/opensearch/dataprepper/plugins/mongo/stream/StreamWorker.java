package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.S3PartitionStatus;
import org.opensearch.dataprepper.plugins.mongo.model.StreamLoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class StreamWorker {
    public static final String STREAM_PREFIX = "STREAM-";
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);
    private static final int DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS = 90_000;
    private static final String COLLECTION_SPLITTER = "\\.";
    static final String SUCCESS_ITEM_COUNTER_NAME = "streamRecordsSuccessTotal";
    static final String FAILURE_ITEM_COUNTER_NAME = "streamRecordsFailedTotal";
    static final String BYTES_RECEIVED = "bytesReceived";
    private final RecordBufferWriter recordBufferWriter;
    private final PartitionKeyRecordConverter recordConverter;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;
    private final MongoDBSourceConfig sourceConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final StreamAcknowledgementManager streamAcknowledgementManager;
    private final  PluginMetrics pluginMetrics;
    private final int recordFlushBatchSize;
    private final int checkPointIntervalInMs;
    private final int bufferWriteIntervalInMs;
    private final int streamBatchSize;
    private boolean stopWorker = false;
    Optional<S3PartitionStatus> s3PartitionStatus = Optional.empty();


    private final JsonWriterSettings writerSettings = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
            .build();

    public static StreamWorker create(final RecordBufferWriter recordBufferWriter,
                         final PartitionKeyRecordConverter recordConverter,
                         final MongoDBSourceConfig sourceConfig,
                         final StreamAcknowledgementManager streamAcknowledgementManager,
                         final DataStreamPartitionCheckpoint partitionCheckpoint,
                         final PluginMetrics pluginMetrics,
                         final int recordFlushBatchSize,
                         final int checkPointIntervalInMs,
                         final int bufferWriteIntervalInMs,
                         final int streamBatchSize
    ) {
        return new StreamWorker(recordBufferWriter, recordConverter, sourceConfig, streamAcknowledgementManager, partitionCheckpoint,
                pluginMetrics, recordFlushBatchSize, checkPointIntervalInMs, bufferWriteIntervalInMs, streamBatchSize);
    }
    public StreamWorker(final RecordBufferWriter recordBufferWriter,
                        final PartitionKeyRecordConverter recordConverter,
                        final MongoDBSourceConfig sourceConfig,
                        final StreamAcknowledgementManager streamAcknowledgementManager,
                        final DataStreamPartitionCheckpoint partitionCheckpoint,
                        final PluginMetrics pluginMetrics,
                        final int recordFlushBatchSize,
                        final int checkPointIntervalInMs,
                        final int bufferWriteIntervalInMs,
                        final int streamBatchSize
                        ) {
        this.recordBufferWriter = recordBufferWriter;
        this.recordConverter  = recordConverter;
        this.sourceConfig = sourceConfig;
        this.streamAcknowledgementManager = streamAcknowledgementManager;
        this.partitionCheckpoint = partitionCheckpoint;
        this.pluginMetrics = pluginMetrics;
        this.recordFlushBatchSize = recordFlushBatchSize;
        this.checkPointIntervalInMs = checkPointIntervalInMs;
        this.bufferWriteIntervalInMs = bufferWriteIntervalInMs;
        this.streamBatchSize = streamBatchSize;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            // starts acknowledgement monitoring thread
            streamAcknowledgementManager.init((Void) -> stop());
        }
    }

    private MongoCursor<ChangeStreamDocument<Document>> getChangeStreamCursor(final MongoCollection<Document> collection,
                            final String resumeToken
                            ) {
        final ChangeStreamIterable<Document> changeStreamIterable = collection.watch().batchSize(streamBatchSize);

        if (resumeToken == null) {
            return changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
        } else {
            return changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(BsonDocument.parse(resumeToken)).maxAwaitTime(60, TimeUnit.SECONDS).iterator();
        }
    }

    private boolean shouldWaitForExport(final StreamPartition streamPartition) {
        final StreamProgressState progressState = streamPartition.getProgressState().get();
        // when export is complete the global load status is created
        final Optional<StreamLoadStatus> loadStatus = partitionCheckpoint.getGlobalStreamLoadStatus();
        return progressState.shouldWaitForExport() && loadStatus.isEmpty();
    }

    private boolean shouldWaitForS3Partition() {
        s3PartitionStatus = partitionCheckpoint.getGlobalS3FolderCreationStatus();
        return s3PartitionStatus.isEmpty();
    }

    public void processStream(final StreamPartition streamPartition) {
        Optional<String> resumeToken = streamPartition.getProgressState().map(StreamProgressState::getResumeToken);
        final String collectionDbName = streamPartition.getCollection();
        List<String> collectionDBNameList = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collectionDBNameList.size() < 2) {
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }
        long recordCount = 0;
        final List<Event> records = new ArrayList<>();
        String checkPointToken = null;
        try (MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            // Access the database
            MongoDatabase database = mongoClient.getDatabase(collectionDBNameList.get(0));

            // Access the collection you want to stream data from
            MongoCollection<Document> collection = database.getCollection(collectionDBNameList.get(1));

            try (MongoCursor<ChangeStreamDocument<Document>> cursor = getChangeStreamCursor(collection, resumeToken.orElse(null))) {
                while ((shouldWaitForExport(streamPartition) || shouldWaitForS3Partition()) && !Thread.currentThread().isInterrupted()) {
                    LOG.info("Initial load not complete for collection {}, waiting for initial lo be complete before resuming streams.", collectionDbName);
                    try {
                        Thread.sleep(DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS);
                    } catch (final InterruptedException ex) {
                        LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                final int totalPartitions = s3PartitionStatus.get().getTotalPartitions();
                if (totalPartitions == 0) {
                    // This should not happen unless the S3 partition creator failed.
                    throw new IllegalStateException("S3 partitions are not created. Please check the S3 partition creator thread.");
                }

                List<String> partitionNames = partitionCheckpoint.getS3FolderPartitions(collectionDbName);
                // Source Coordinator is eventually consistent. It may take some time for all partitions to be queried.
                while (totalPartitions != partitionNames.size() && !Thread.currentThread().isInterrupted()) {
                    Thread.sleep(15_000);
                    partitionNames = partitionCheckpoint.getS3FolderPartitions(collectionDbName);
                }

                recordConverter.initializePartitions(partitionNames);
                long lastCheckpointTime = System.currentTimeMillis();
                long lastBufferWriteTime = lastCheckpointTime;
                while (cursor.hasNext() && !Thread.currentThread().isInterrupted() && !stopWorker) {
                    try {
                        final ChangeStreamDocument<Document> document = cursor.next();
                        final String record = document.getFullDocument().toJson(writerSettings);
                        final long eventCreationTimeMillis = document.getClusterTime().getTime() * 1000L;
                        final long bytes = record.getBytes().length;
                        bytesReceivedSummary.record(bytes);

                        checkPointToken = document.getResumeToken().toJson(writerSettings);
                        // TODO fix eventVersionNumber
                        final Event event = recordConverter.convert(record, eventCreationTimeMillis, eventCreationTimeMillis, document.getOperationTypeString());

                        records.add(event);
                        recordCount += 1;

                        if ((recordCount % recordFlushBatchSize == 0) || (System.currentTimeMillis() - lastBufferWriteTime >= bufferWriteIntervalInMs)) {
                            LOG.debug("Write to buffer for line {} to {}", (recordCount - recordFlushBatchSize), recordCount);
                            writeToBuffer(records, checkPointToken, recordCount);
                            lastBufferWriteTime = System.currentTimeMillis();
                            records.clear();
                            if (!sourceConfig.isAcknowledgmentsEnabled() && (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs)) {
                                LOG.debug("Perform regular checkpointing for resume token {} at record count {}", checkPointToken, recordCount);
                                partitionCheckpoint.checkpoint(checkPointToken, recordCount);
                                lastCheckpointTime = System.currentTimeMillis();
                            }
                        }
                    } catch (Exception e) {
                        // TODO handle documents with size > 10 MB.
                        // this will only happen if writing to buffer gets interrupted from shutdown,
                        // otherwise it's infinite backoff and retry
                        LOG.error("Failed to add records to buffer with error {}", e.getMessage());
                        failureItemsCounter.increment(records.size());
                    }
                }
            }
        } catch (final Exception e) {
            LOG.error("Exception connecting to cluster and processing stream", e);
            throw new RuntimeException(e);
        } finally {
            if (!records.isEmpty()) {
                LOG.info("Flushing and checkpointing last processed record batch from the stream before terminating");
                writeToBuffer(records, checkPointToken, recordCount);
            }
            // Do final checkpoint.
            if (!sourceConfig.isAcknowledgmentsEnabled()) {
                partitionCheckpoint.checkpoint(checkPointToken, recordCount);
            }

            // shutdown acknowledgement monitoring thread
            if (streamAcknowledgementManager != null) {
                streamAcknowledgementManager.shutdown();
            }
        }
    }

    private void writeToBuffer(final List<Event> records, final String checkPointToken, final long recordCount) {
        final AcknowledgementSet acknowledgementSet = streamAcknowledgementManager.createAcknowledgementSet(checkPointToken, recordCount).orElse(null);
        recordBufferWriter.writeToBuffer(acknowledgementSet, records);
        successItemsCounter.increment(records.size());
    }

    void stop() {
        stopWorker = true;
    }
}
