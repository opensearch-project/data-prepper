package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final ExecutorService executorService;
    private String lastLocalCheckpoint;
    private Long lastLocalRecordCount = null;
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
        this.executorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("mongodb-stream-checkpoint"));
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            // starts acknowledgement monitoring thread
            streamAcknowledgementManager.init((Void) -> stop());
        } else {
            // checkpoint in separate thread
            this.executorService.submit(this::checkpointStream);
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

    private boolean shouldWaitForS3Partition(final String collection) {
        s3PartitionStatus = partitionCheckpoint.getGlobalS3FolderCreationStatus(collection);
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
            MongoCollection<Document> collection = database.getCollection(collectionDbName.substring(collectionDBNameList.get(0).length() + 1));

            try (MongoCursor<ChangeStreamDocument<Document>> cursor = getChangeStreamCursor(collection, resumeToken.orElse(null))) {
                while ((shouldWaitForExport(streamPartition) || shouldWaitForS3Partition(streamPartition.getCollection())) && !Thread.currentThread().isInterrupted()) {
                    LOG.info("Initial load not complete for collection {}, waiting for initial lo be complete before resuming streams.", collectionDbName);
                    try {
                        Thread.sleep(DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS);
                    } catch (final InterruptedException ex) {
                        LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
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
                long lastBufferWriteTime = System.currentTimeMillis();
                while (!Thread.currentThread().isInterrupted() && !stopWorker) {
                    if (cursor.hasNext()) {
                        try {
                            final ChangeStreamDocument<Document> document = cursor.next();
                            final OperationType operationType = document.getOperationType();
                            LOG.debug("Event Operation type {}", operationType);
                            if (isCRUDOperation(operationType)) {
                                final String record;
                                if (OperationType.DELETE == operationType) {
                                    record = document.getDocumentKey().toJson(writerSettings);
                                } else {
                                    record = document.getFullDocument().toJson(writerSettings);
                                }
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
                                    lastLocalCheckpoint = checkPointToken;
                                    lastLocalRecordCount = recordCount;
                                    lastBufferWriteTime = System.currentTimeMillis();
                                    records.clear();
                                }
                            } else if(shouldTerminateChangeStream(operationType)){
                                stop();
                                partitionCheckpoint.resetCheckpoint();
                                LOG.warn("The change stream is invalid due to stream operation type {}. Stopping the change stream.", operationType);
                            } else {
                                LOG.warn("The change stream operation type {} is not handled", operationType);
                            }
                        } catch(Exception e){
                            // TODO handle documents with size > 10 MB.
                            // this will only happen if writing to buffer gets interrupted from shutdown,
                            // otherwise it's infinite backoff and retry
                            LOG.error("Failed to add records to buffer with error", e);
                            failureItemsCounter.increment(records.size());
                        }
                    } else {
                        LOG.warn("The change stream cursor didn't return any document. Stopping the change stream.");
                        stop();
                        partitionCheckpoint.resetCheckpoint();
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

    private boolean isCRUDOperation(final OperationType operationType) {
        return OperationType.INSERT == operationType ||
                OperationType.DELETE == operationType ||
                OperationType.UPDATE == operationType ||
                OperationType.REPLACE == operationType;
    }

    private boolean shouldTerminateChangeStream(final OperationType operationType) {
        return OperationType.INVALIDATE == operationType ||
                OperationType.DROP == operationType ||
                OperationType.DROP_DATABASE == operationType;
    }

    private void writeToBuffer(final List<Event> records, final String checkPointToken, final long recordCount) {
        final AcknowledgementSet acknowledgementSet = streamAcknowledgementManager.createAcknowledgementSet(checkPointToken, recordCount).orElse(null);
        recordBufferWriter.writeToBuffer(acknowledgementSet, records);
        successItemsCounter.increment(records.size());
    }

    private void checkpointStream() {
        long lastCheckpointTime = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted() && !stopWorker) {
            if (lastLocalRecordCount != null && (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs)) {
                LOG.debug("Perform regular checkpoint for resume token {} at record count {}", lastLocalCheckpoint, lastLocalRecordCount);
                partitionCheckpoint.checkpoint(lastLocalCheckpoint, lastLocalRecordCount);
                lastCheckpointTime = System.currentTimeMillis();
            }

            try {
                Thread.sleep(checkPointIntervalInMs);
            } catch (InterruptedException ex) {
                break;
            }
        }
        LOG.info("Checkpoint monitoring thread interrupted.");
    }

    void stop() {
        stopWorker = true;
    }
}
