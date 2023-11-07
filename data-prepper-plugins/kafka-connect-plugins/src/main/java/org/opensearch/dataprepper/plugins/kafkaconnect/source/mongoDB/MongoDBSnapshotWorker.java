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
import org.bson.types.ObjectId;
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

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

public class MongoDBSnapshotWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSnapshotWorker.class);
    private static final Duration BACKOFF_ON_EXCEPTION = Duration.ofSeconds(60);
    private static final Duration BACKOFF_ON_EMPTY_PARTITION = Duration.ofSeconds(60);
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    private static final String SUCCESS_ITEM_COUNTER_NAME = "exportRecordsSuccessTotal";
    private static final String SUCCESS_PARTITION_COUNTER_NAME = "exportPartitionSuccessTotal";
    private static final String FAILURE_PARTITION_COUNTER_NAME = "exportPartitionFailureTotal";
    private static final String EVENT_SOURCE_COLLECTION_ATTRIBUTE = "__collection";
    private static final String EVENT_SOURCE_DB_ATTRIBUTE = "__source_db";
    private static final String EVENT_SOURCE_OPERATION = "__op";
    private static final String EVENT_TYPE = "EXPORT";
    private static int DEFAULT_BUFFER_WRITE_TIMEOUT_MS = 5000;
    private final SourceCoordinator<MongoDBSnapshotProgressState> sourceCoordinator;
    private final Buffer<Record<Object>> buffer;
    private final MongoDBPartitionCreationSupplier mongoDBPartitionCreationSupplier;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final MongoDBConfig mongoDBConfig;
    private final Counter successItemsCounter;
    private final Counter successPartitionCounter;
    private final Counter failureParitionCounter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {};


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
        List<String> partitionKeys = List.of(partition.getPartitionKey().split("\\|"));
        if (partitionKeys.size() < 4) {
            throw new RuntimeException("Invalid Partition Key. Must as db.collection|gte|lte format.");
        }
        List<String> collection = List.of(partitionKeys.get(0).split("\\."));
        final String gte = partitionKeys.get(1);
        final String lte = partitionKeys.get(2);
        final String className = partitionKeys.get(3);
        if (collection.size() < 2) {
            throw new RuntimeException("Invalid Collection Name. Must as db.collection format");
        }
        MongoClient mongoClient = MongoDBHelper.getMongoClient(mongoDBConfig);
        MongoDatabase db = mongoClient.getDatabase(collection.get(0));
        MongoCollection<Document> col = db.getCollection(collection.get(1));
        Bson query = this.buildQuery(gte, lte, className);
        MongoCursor<Document> cursor = col.find(query).iterator();
        int totalRecords = 0;
        try {
            while (cursor.hasNext()) {

                String record = cursor.next().toJson();
                Map<String, Object> data = convertToMap(record);
                data.putIfAbsent(EVENT_SOURCE_DB_ATTRIBUTE, collection.get(0));
                data.putIfAbsent(EVENT_SOURCE_COLLECTION_ATTRIBUTE, collection.get(1));
                data.putIfAbsent(EVENT_SOURCE_OPERATION, OpenSearchBulkActions.CREATE.toString());
                if (buffer.isByteBuffer()) {
                    buffer.writeBytes(objectMapper.writeValueAsBytes(data), null, DEFAULT_BUFFER_WRITE_TIMEOUT_MS);
                } else {
                    buffer.write(getEventFromData(data), DEFAULT_BUFFER_WRITE_TIMEOUT_MS);
                }
                successItemsCounter.increment();
                totalRecords += 1;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cursor.close();
        }
        final MongoDBSnapshotProgressState progressState = new MongoDBSnapshotProgressState();
        progressState.setTotal(totalRecords);
        sourceCoordinator.saveProgressStateForPartition(partition.getPartitionKey(), progressState);
    }

    private Bson buildQuery(String gte, String lte, String className) {
        switch (className) {
            case "java.lang.Integer":
                return and(
                        gte("_id", Integer.parseInt(gte)),
                        lte("_id", Integer.parseInt(lte))
                );
            case "java.lang.Double":
                return and(
                        gte("_id", Double.parseDouble(gte)),
                        lte("_id", Double.parseDouble(lte))
                );
            case "java.lang.String":
                return and(
                        gte("_id", gte),
                        lte("_id", lte)
                );
            case "java.lang.Long":
                return and(
                        gte("_id", Long.parseLong(gte)),
                        lte("_id", Long.parseLong(lte))
                );
            case "org.bson.types.ObjectId":
                return and(
                        gte("_id", new ObjectId(gte)),
                        lte("_id", new ObjectId(lte))
                );
            default:
                throw new RuntimeException("Unexpected _id class supported: " + className);
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

    private Map<String, Object> convertToMap(String jsonData) {
        try {
            return objectMapper.readValue(jsonData, mapTypeReference);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private Record<Object> getEventFromData(Map<String, Object> data) {
        Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(data)
                .build();
        return new Record<>(event);
    }
}