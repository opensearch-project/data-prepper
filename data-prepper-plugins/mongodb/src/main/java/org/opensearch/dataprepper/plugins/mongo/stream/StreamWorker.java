package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.micrometer.core.instrument.Counter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;
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

    private final RecordBufferWriter recordBufferWriter;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;
    private final MongoDBSourceConfig sourceConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final  PluginMetrics pluginMetrics;
    private final int defaultFlushBatchSize;

    private final JsonWriterSettings writerSettings = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED)
            .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
            .build();

    public static StreamWorker create(final RecordBufferWriter recordBufferWriter,
                         final AcknowledgementSetManager acknowledgementSetManager,
                         final MongoDBSourceConfig sourceConfig,
                         final DataStreamPartitionCheckpoint partitionCheckpoint,
                         final PluginMetrics pluginMetrics,
                         final int defaultFlushBatchSize
    ) {
        return new StreamWorker(recordBufferWriter, acknowledgementSetManager,
                sourceConfig, partitionCheckpoint, pluginMetrics, defaultFlushBatchSize);
    }
    public StreamWorker(final RecordBufferWriter recordBufferWriter,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final MongoDBSourceConfig sourceConfig,
                        final DataStreamPartitionCheckpoint partitionCheckpoint,
                        final PluginMetrics pluginMetrics,
                        final int defaultFlushBatchSize
                        ) {
        this.recordBufferWriter = recordBufferWriter;
        this.sourceConfig = sourceConfig;
        this.partitionCheckpoint = partitionCheckpoint;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.defaultFlushBatchSize = defaultFlushBatchSize;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
    }

    private MongoCursor<ChangeStreamDocument<Document>> getChangeStreamCursor(final MongoCollection<Document> collection,
                            final String resumeToken
                            ) {
        if (resumeToken == null) {
            return collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
        } else {
            return collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(BsonDocument.parse(resumeToken)).maxAwaitTime(60, TimeUnit.SECONDS).iterator();
        }
    }

    private boolean shouldWaitForExport(final StreamPartition streamPartition) {
        final StreamProgressState progressState = streamPartition.getProgressState().get();
        // when export is complete the global load status is created
        final Optional<StreamLoadStatus> loadStatus = partitionCheckpoint.getGlobalStreamLoadStatus();
        return progressState.shouldWaitForExport() && loadStatus.isEmpty();
    }

    public void processStream(final StreamPartition streamPartition) {
        Optional<String> resumeToken = streamPartition.getProgressState().map(StreamProgressState::getResumeToken);
        final String collectionDbName = streamPartition.getCollection();
        List<String> collectionDBNameList = List.of(collectionDbName.split(COLLECTION_SPLITTER));
        if (collectionDBNameList.size() < 2) {
            throw new IllegalArgumentException("Invalid Collection Name. Must be in db.collection format");
        }
        int recordCount = 0;
        final List<String> records = new ArrayList<>();
        // TODO: create acknowledgementSet
        AcknowledgementSet acknowledgementSet = null;
        String checkPointToken = null;
        try (MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            // Access the database
            MongoDatabase database = mongoClient.getDatabase(collectionDBNameList.get(0));

            // Access the collection you want to stream data from
            MongoCollection<Document> collection = database.getCollection(collectionDBNameList.get(1));

            try (MongoCursor<ChangeStreamDocument<Document>> cursor = getChangeStreamCursor(collection, resumeToken.orElse(null))) {
                while (shouldWaitForExport(streamPartition) && !Thread.currentThread().isInterrupted()) {
                    LOG.info("Initial load not complete for collection {}, waiting for initial lo be complete before resuming streams.", collectionDbName);
                    try {
                        Thread.sleep(DEFAULT_EXPORT_COMPLETE_WAIT_INTERVAL_MILLIS);
                    } catch (final InterruptedException ex) {
                        LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                        break;
                    }
                }

                while (cursor.hasNext() && !Thread.currentThread().isInterrupted()) {
                    try {
                        final ChangeStreamDocument<Document> document = cursor.next();
                        final String record = document.getFullDocument().toJson(writerSettings);

                        checkPointToken = document.getResumeToken().toJson(writerSettings);

                        records.add(record);
                        recordCount += 1;

                        if (recordCount % defaultFlushBatchSize == 0) {
                            LOG.debug("Write to buffer for line " + (recordCount - defaultFlushBatchSize) + " to " + recordCount);
                            recordBufferWriter.writeToBuffer(acknowledgementSet, records);
                            records.clear();
                            LOG.debug("Perform regular checkpointing for stream Loader");
                            partitionCheckpoint.checkpoint(checkPointToken, recordCount);
                            successItemsCounter.increment();
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to add record to buffer with error {}", e.getMessage());
                        failureItemsCounter.increment();
                    }
                }
            }
        } catch (final Exception e) {
            LOG.error("Exception connecting to cluster and processing stream", e);
            throw new RuntimeException(e);
        } finally {
            LOG.info("Checkpointing processing stream");
            if (!records.isEmpty()) {
                recordBufferWriter.writeToBuffer(acknowledgementSet, records);
            }
            // Do final checkpoint.
            partitionCheckpoint.checkpoint(checkPointToken, recordCount);
        }
    }
}
