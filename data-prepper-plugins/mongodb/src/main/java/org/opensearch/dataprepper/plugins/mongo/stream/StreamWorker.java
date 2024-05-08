package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.MongoClientException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.bson.BsonDocument;
import org.bson.Document;
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
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.opensearch.dataprepper.model.source.s3.S3ScanEnvironmentVariables.STOP_S3_SCAN_PROCESSING_PROPERTY;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.JSON_WRITER_SETTINGS;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.DOCUMENTDB_ID_FIELD_NAME;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.UNKNOWN_TYPE;

public class StreamWorker {
    public static final String STREAM_PREFIX = "STREAM-";
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);
    private static final int DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS = 90_000;
    private static final String COLLECTION_SPLITTER = "\\.";
    private static final Set<OperationType> CRUD_OPERATION_TYPE = Set.of(OperationType.INSERT, OperationType.DELETE,
            OperationType.UPDATE, OperationType.REPLACE);
    private static final Set<OperationType> STREAM_TERMINATE_OPERATION_TYPE = Set.of(OperationType.INVALIDATE,
            OperationType.DROP, OperationType.DROP_DATABASE);
    static final String SUCCESS_ITEM_COUNTER_NAME = "changeEventsProcessed";
    static final String FAILURE_ITEM_COUNTER_NAME = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    private static final long MILLI_SECOND = 1_000_000L;
    private static final String UPDATE_DESCRIPTION = "updateDescription";
    private static final int BUFFER_WRITE_TIMEOUT_MILLIS = 15_000;
    private final RecordBufferWriter recordBufferWriter;
    private final PartitionKeyRecordConverter recordConverter;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;
    private final MongoDBSourceConfig sourceConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    private final StreamAcknowledgementManager streamAcknowledgementManager;
    private final  PluginMetrics pluginMetrics;
    private final int recordFlushBatchSize;
    private final int checkPointIntervalInMs;
    private final int bufferWriteIntervalInMs;
    private final int streamBatchSize;
    private final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics;
    private boolean stopWorker = false;
    private final ExecutorService executorService;
    private String lastLocalCheckpoint = null;
    private long lastLocalRecordCount = 0;
    Optional<S3PartitionStatus> s3PartitionStatus = Optional.empty();
    private Integer currentEpochSecond;
    private int recordsSeenThisSecond = 0;
    final List<Event> records = new ArrayList<>();
    final List<Long> recordBytes = new ArrayList<>();
    long lastBufferWriteTime = System.currentTimeMillis();
    private String checkPointToken = null;
    private long recordCount = 0;
    private final Lock lock;

    public static StreamWorker create(final RecordBufferWriter recordBufferWriter,
                         final PartitionKeyRecordConverter recordConverter,
                         final MongoDBSourceConfig sourceConfig,
                         final StreamAcknowledgementManager streamAcknowledgementManager,
                         final DataStreamPartitionCheckpoint partitionCheckpoint,
                         final PluginMetrics pluginMetrics,
                         final int recordFlushBatchSize,
                         final int checkPointIntervalInMs,
                         final int bufferWriteIntervalInMs,
                         final int streamBatchSize,
                         final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics
    ) {
        return new StreamWorker(recordBufferWriter, recordConverter, sourceConfig, streamAcknowledgementManager, partitionCheckpoint,
                pluginMetrics, recordFlushBatchSize, checkPointIntervalInMs, bufferWriteIntervalInMs, streamBatchSize, documentDBAggregateMetrics);
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
                        final int streamBatchSize,
                        final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics
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
        this.documentDBAggregateMetrics = documentDBAggregateMetrics;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.executorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("mongodb-stream-checkpoint"));
        this.lock = new ReentrantLock();
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            // starts acknowledgement monitoring thread
            streamAcknowledgementManager.init((Void) -> stop());
        }
        // buffer write and checkpoint in separate thread on timeout
        this.executorService.submit(this::bufferWriteAndCheckpointStream);

    }

    private MongoCursor<ChangeStreamDocument<Document>> getChangeStreamCursor(final MongoCollection<Document> collection,
                            final String resumeToken
                            ) {
        final ChangeStreamIterable<Document> changeStreamIterable = collection.watch(
                        List.of(Aggregates.project(Projections.exclude(UPDATE_DESCRIPTION))))
                .batchSize(streamBatchSize);

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
        documentDBAggregateMetrics.getStreamApiInvocations().increment();

        Optional<String> resumeToken = streamPartition.getProgressState().map(StreamProgressState::getResumeToken);
        resumeToken.ifPresent(token -> checkPointToken = token);
        Optional<Long> loadedRecords = streamPartition.getProgressState().map(StreamProgressState::getLoadedRecords);
        loadedRecords.ifPresent(count -> recordCount = count);

        final String collectionDbName = streamPartition.getCollection();
        List<String> collectionDBNameList = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collectionDBNameList.size() < 2) {
            documentDBAggregateMetrics.getStream4xxErrors().increment();
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }

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
                    documentDBAggregateMetrics.getStream5xxErrors().increment();
                    throw new IllegalStateException("S3 partitions are not created. Please check the S3 partition creator thread.");
                }
                recordConverter.initializePartitions(s3Partitions);
                while (!Thread.currentThread().isInterrupted() && !stopWorker) {
                    if (cursor.hasNext()) {
                        try {
                            final ChangeStreamDocument<Document> document = cursor.next();
                            final OperationType operationType = document.getOperationType();
                            LOG.debug("Event Operation type {}", operationType);
                            if (isCRUDOperation(operationType)) {
                                final String record;
                                if (OperationType.DELETE == operationType) {
                                    record = document.getDocumentKey().toJson(JSON_WRITER_SETTINGS);
                                } else {
                                    record = document.getFullDocument().toJson(JSON_WRITER_SETTINGS);
                                }
                                final long eventCreateTimeEpochMillis = document.getClusterTime().getTime() * 1_000L;
                                final long eventCreationTimeEpochNanos = calculateTieBreakingVersionFromTimestamp(document.getClusterTime().getTime());
                                final long bytes = record.getBytes().length;
                                bytesReceivedSummary.record(bytes);

                                final Optional<BsonDocument> primaryKeyDoc = Optional.ofNullable(document.getDocumentKey());
                                final String primaryKeyBsonType = primaryKeyDoc.map(bsonDocument -> bsonDocument.get(DOCUMENTDB_ID_FIELD_NAME).getBsonType().name()).orElse(UNKNOWN_TYPE);
                                final Event event = recordConverter.convert(record, eventCreateTimeEpochMillis, eventCreationTimeEpochNanos,
                                        document.getOperationType(), primaryKeyBsonType);
                                if (sourceConfig.getIdKey() !=null && !sourceConfig.getIdKey().isBlank()) {
                                    event.put(sourceConfig.getIdKey(), event.get(DOCUMENTDB_ID_FIELD_NAME, Object.class));
                                }
                                // delete _id
                                event.delete(DOCUMENTDB_ID_FIELD_NAME);
                                records.add(event);
                                recordBytes.add(bytes);

                                lock.lock();
                                try {
                                    recordCount += 1;
                                    checkPointToken = document.getResumeToken().toJson(JSON_WRITER_SETTINGS);

                                    if ((recordCount % recordFlushBatchSize == 0) || (System.currentTimeMillis() - lastBufferWriteTime >= bufferWriteIntervalInMs)) {
                                        writeToBuffer();
                                    }
                                } finally {
                                    lock.unlock();
                                }

                            } else if(shouldTerminateChangeStream(operationType)){
                                stop();
                                partitionCheckpoint.resetCheckpoint();
                                LOG.warn("The change stream is invalid due to stream operation type {}. Stopping the current change stream. New thread should restart the stream.", operationType);
                            } else {
                                LOG.warn("The change stream operation type {} is not handled", operationType);
                            }
                        } catch(Exception e){
                            // this will only happen if writing to buffer gets interrupted from shutdown,
                            // otherwise it's infinite backoff and retry
                            LOG.error("Failed to add records to buffer with error", e);
                            failureItemsCounter.increment(records.size());
                        }
                    } else {
                        LOG.warn("The change stream cursor didn't return any document. Stopping the change stream. New thread should restart the stream.");
                        stop();
                        partitionCheckpoint.resetCheckpoint();
                    }
                }
            }
        } catch (final IllegalArgumentException | MongoClientException e) {
            // IllegalArgumentException is thrown when database or collection name is not valid
            // MongoClientException is thrown for exceptions indicating a failure condition with the MongoClient
            documentDBAggregateMetrics.getStream4xxErrors().increment();
            LOG.error("Client side exception connecting to cluster and processing stream", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            documentDBAggregateMetrics.getStream5xxErrors().increment();
            LOG.error("Server side exception connecting to cluster and processing stream", e);
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

            System.clearProperty(STOP_S3_SCAN_PROCESSING_PROPERTY);

            // stop other threads for this worker
            stop();

            partitionCheckpoint.giveUpPartition();

            // shutdown acknowledgement monitoring thread
            if (streamAcknowledgementManager != null) {
                streamAcknowledgementManager.shutdown();
            }
        }
    }

    private long calculateTieBreakingVersionFromTimestamp(final int eventTimeInEpochSeconds) {
        if (currentEpochSecond == null) {
            currentEpochSecond = eventTimeInEpochSeconds;
        } else if (currentEpochSecond > eventTimeInEpochSeconds) {
            return eventTimeInEpochSeconds * MILLI_SECOND;
        } else if (currentEpochSecond < eventTimeInEpochSeconds) {
            recordsSeenThisSecond = 0;
            currentEpochSecond = eventTimeInEpochSeconds;
        } else {
            recordsSeenThisSecond++;
        }

        return eventTimeInEpochSeconds * MILLI_SECOND + recordsSeenThisSecond;
    }

    private boolean isCRUDOperation(final OperationType operationType) {
        return CRUD_OPERATION_TYPE.contains(operationType);
    }

    private boolean shouldTerminateChangeStream(final OperationType operationType) {
        return STREAM_TERMINATE_OPERATION_TYPE.contains(operationType);
    }

    private void writeToBuffer(final List<Event> records, final String checkPointToken, final long recordCount) {
        final AcknowledgementSet acknowledgementSet = streamAcknowledgementManager.createAcknowledgementSet(checkPointToken, recordCount).orElse(null);
        recordBufferWriter.writeToBuffer(acknowledgementSet, records);
        successItemsCounter.increment(records.size());
        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }
    }

    private void writeToBuffer() {
        LOG.debug("Write to buffer for line {} to {}", lastLocalRecordCount, recordCount);
        writeToBuffer(records, checkPointToken, recordCount);
        lastLocalCheckpoint = checkPointToken;
        lastLocalRecordCount = recordCount;
        lastBufferWriteTime = System.currentTimeMillis();
        bytesProcessedSummary.record(recordBytes.stream().mapToLong(Long::longValue).sum());
        records.clear();
        recordBytes.clear();
    }

    private void bufferWriteAndCheckpointStream() {
        long lastCheckpointTime = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted() && !stopWorker) {
            if (!records.isEmpty() && lastBufferWriteTime < Instant.now().minusMillis(BUFFER_WRITE_TIMEOUT_MILLIS).toEpochMilli()) {
                lock.lock();
                LOG.debug("Writing to buffer due to buffer write delay");
                try {
                    writeToBuffer();
                } catch(Exception e){
                    // this will only happen if writing to buffer gets interrupted from shutdown,
                    // otherwise it's infinite backoff and retry
                    LOG.error("Failed to add records to buffer with error", e);
                    failureItemsCounter.increment(records.size());
                } finally {
                    lock.unlock();
                }
            }

            if (!sourceConfig.isAcknowledgmentsEnabled()) {
                if (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs) {
                    try {
                        lock.lock();
                        LOG.debug("Perform regular checkpoint for resume token {} at record count {}", lastLocalCheckpoint, lastLocalRecordCount);
                        partitionCheckpoint.checkpoint(lastLocalCheckpoint, lastLocalRecordCount);
                    } catch (Exception e) {
                        LOG.warn("Exception checkpointing the current state. The stream record processing will start from previous checkpoint.", e);
                        stop();
                    } finally {
                        lock.unlock();;
                    }
                    lastCheckpointTime = System.currentTimeMillis();
                }
            }

            try {
                Thread.sleep(BUFFER_WRITE_TIMEOUT_MILLIS);
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
