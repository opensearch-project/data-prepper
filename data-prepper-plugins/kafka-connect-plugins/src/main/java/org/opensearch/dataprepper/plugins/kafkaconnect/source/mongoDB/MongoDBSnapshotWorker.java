/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.source.mongoDB;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.micrometer.core.instrument.Counter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartition;
import org.opensearch.dataprepper.plugins.kafkaconnect.configuration.MongoDBConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongoDBSnapshotWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotWorker.class);
    private static final Duration BACKOFF_ON_EXCEPTION = Duration.ofSeconds(60);
    private static final Duration BACKOFF_ON_EMPTY_PARTITION = Duration.ofSeconds(60);
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    private static final String SUCCESS_ITEM_COUNTER_NAME = "exportRecordsSuccessTotal";
    private static final String FAILURE_ITEM_COUNTER_NAME = "exportRecordsFailedTotal";
    private static final String SUCCESS_PARTITION_COUNTER_NAME = "exportPartitionSuccessTotal";
    private static final String FAILURE_PARTITION_COUNTER_NAME = "exportPartitionFailureTotal";
    private static final String EVENT_SOURCE_COLLECTION_ATTRIBUTE = "__collection";
    private static final String EVENT_SOURCE_DB_ATTRIBUTE = "__source_db";
    private static final String EVENT_SOURCE_OPERATION = "__op";
    private static final String EVENT_SOURCE_TS_MS = "__source_ts_ms";
    private static final String EVENT_TYPE = "EXPORT";
    private static final String PARTITION_KEY_SPLITTER = "\\|";
    private static final String COLLECTION_SPLITTER = "\\.";
    private final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;
    private static int DEFAULT_BUFFER_WRITE_TIMEOUT_MS = 5000;
    private final Buffer<Record<Object>> buffer;
    private final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final MongoDBConfig mongoDBConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final Counter successPartitionCounter;
    private final Counter failureParitionCounter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {
    };


    public MongoDBSnapshotWorker(final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator,
                                 final Buffer<Record<Object>> buffer,
                                 final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier,
                                 final PluginMetrics pluginMetrics,
                                 final AcknowledgementSetManager acknowledgementSetManager,
                                 final MongoDBConfig mongoDBConfig) {
        this.sourceCoordinator = sourceCoordinator;
        this.buffer = buffer;
        this.mongoDBPartitionCreationSupplier = mongoDBPartitionCreationSupplier;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.mongoDBConfig = mongoDBConfig;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.successPartitionCounter = pluginMetrics.counter(SUCCESS_PARTITION_COUNTER_NAME);
        this.failureParitionCounter = pluginMetrics.counter(FAILURE_PARTITION_COUNTER_NAME);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<SourcePartition<MongoDBSnapshotProgressState>> snapshotPartition = sourceCoordinator.getNextPartition(mongoDBPartitionCreationSupplier);
                if (snapshotPartition.isEmpty()) {
                    try {
                        LOG.info("get empty partition");
                        Thread.sleep(BACKOFF_ON_EMPTY_PARTITION.toMillis());
                        continue;
                    } catch (final InterruptedException e) {
                        LOG.info("The worker was interrupted while sleeping after acquiring no indices to process, stopping processing");
                        return;
                    }
                }
                LOG.info("get partition success {}", snapshotPartition.get().getPartitionKey());
                try {
                    final Optional<AcknowledgementSet> acknowledgementSet = createAcknowledgementSet(snapshotPartition.get());
                    this.startProcessPartition(snapshotPartition.get());
                    if (acknowledgementSet.isEmpty()) {
                        sourceCoordinator.completePartition(snapshotPartition.get().getPartitionKey(), false);
                    } else {
                        sourceCoordinator.updatePartitionForAcknowledgmentWait(snapshotPartition.get().getPartitionKey(), ACKNOWLEDGEMENT_SET_TIMEOUT);
                        acknowledgementSet.get().complete();
                    }
                    successPartitionCounter.increment();
                } catch (final Exception e) {
                    LOG.error("Received an exception while processing the partition.", e);
                    sourceCoordinator.giveUpPartitions();
                    failureParitionCounter.increment();
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while trying to snapshot documentDB, backing off and retrying", e);
                try {
                    Thread.sleep(BACKOFF_ON_EXCEPTION.toMillis());
                } catch (final InterruptedException ex) {
                    LOG.info("The DocumentDBSnapshotWorker was interrupted before backing off and retrying, stopping processing");
                    return;
                }
            }
        }
    }

    private void startProcessPartition(SourcePartition<MongoDBSnapshotProgressState> partition) {
        List<String> partitionKeys = List.of(partition.getPartitionKey().split(PARTITION_KEY_SPLITTER));
        if (partitionKeys.size() < 4) {
            throw new RuntimeException("Invalid Partition Key. Must as db.collection|gte|lte format.");
        }
        List<String> collection = List.of(partitionKeys.get(0).split(COLLECTION_SPLITTER));
        final String gte = partitionKeys.get(1);
        final String lte = partitionKeys.get(2);
        final String className = partitionKeys.get(3);
        if (collection.size() < 2) {
            throw new RuntimeException("Invalid Collection Name. Must as db.collection format");
        }
        try (MongoClient mongoClient = MongoDBHelper.getMongoClient(mongoDBConfig)) {
            MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            MongoCollection<Document> col = db.getCollection(collection.get(1));
            Bson query = MongoDBHelper.buildAndQuery(gte, lte, className);
            long totalRecords = 0L;
            long successRecords = 0L;
            long failedRecords = 0L;
            try (MongoCursor<Document> cursor = col.find(query).iterator()) {
                while (cursor.hasNext()) {
                    totalRecords += 1;
                    try {
                        JsonWriterSettings writerSettings = JsonWriterSettings.builder()
                                .outputMode(JsonMode.RELAXED)
                                .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
                                .build();
                        String record = cursor.next().toJson(writerSettings);
                        Map<String, Object> data = convertToMap(record);
                        data.putIfAbsent(EVENT_SOURCE_DB_ATTRIBUTE, collection.get(0));
                        data.putIfAbsent(EVENT_SOURCE_COLLECTION_ATTRIBUTE, collection.get(1));
                        data.putIfAbsent(EVENT_SOURCE_OPERATION, OpenSearchBulkActions.CREATE.toString());
                        data.putIfAbsent(EVENT_SOURCE_TS_MS, 0);
                        if (buffer.isByteBuffer()) {
                            buffer.writeBytes(objectMapper.writeValueAsBytes(data), null, DEFAULT_BUFFER_WRITE_TIMEOUT_MS);
                        } else {
                            buffer.write(getEventFromData(data), DEFAULT_BUFFER_WRITE_TIMEOUT_MS);
                        }
                        successItemsCounter.increment();
                        successRecords += 1;
                    } catch (Exception e) {
                        LOG.error("failed to add record to buffer with error {}", e.getMessage());
                        failureItemsCounter.increment();
                        failedRecords += 1;
                    }
                }
                final MongoDBSnapshotProgressState progressState = new MongoDBSnapshotProgressState();
                progressState.setTotal(totalRecords);
                progressState.setSuccess(successRecords);
                progressState.setFailure(failedRecords);
                sourceCoordinator.saveProgressStateForPartition(partition.getPartitionKey(), progressState);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Optional<AcknowledgementSet> createAcknowledgementSet(SourcePartition<MongoDBSnapshotProgressState> partition) {
        if (mongoDBConfig.getExportConfig().getAcknowledgements()) {
            return Optional.of(this.acknowledgementSetManager.create((result) -> {
                if (result) {
                    this.sourceCoordinator.completePartition(partition.getPartitionKey(), true);
                }
            }, ACKNOWLEDGEMENT_SET_TIMEOUT));
        }
        return Optional.empty();
    }

    private Map<String, Object> convertToMap(String jsonData) throws JsonProcessingException {
        return objectMapper.readValue(jsonData, mapTypeReference);
    }

    private Record<Object> getEventFromData(Map<String, Object> data) {
        Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(data)
                .build();
        return new Record<>(event);
    }
}
