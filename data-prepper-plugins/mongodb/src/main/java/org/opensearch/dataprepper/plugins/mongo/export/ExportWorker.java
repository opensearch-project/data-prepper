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
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.client.BsonHelper;
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.DataQueryProgressState;
import org.opensearch.dataprepper.plugins.mongo.model.LoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler.EXPORT_PREFIX;

public class ExportWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ExportWorker.class);
    public static final String STREAM_PREFIX = "STREAM-";
    private static final int PARTITION_KEY_PARTS = 4;
    private final AtomicInteger numOfWorkers = new AtomicInteger(0);

    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    static final String SUCCESS_ITEM_COUNTER_NAME = "exportRecordsSuccessTotal";
    static final String FAILURE_ITEM_COUNTER_NAME = "exportRecordsFailedTotal";
    static final String SUCCESS_PARTITION_COUNTER_NAME = "exportPartitionSuccessTotal";
    static final String FAILURE_PARTITION_COUNTER_NAME = "exportPartitionFailureTotal";
    static final String ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE = "activeExportPartitionConsumers";
    private static final String PARTITION_KEY_SPLITTER = "\\|";
    private static final String COLLECTION_SPLITTER = "\\.";

    /**
     * Maximum concurrent data loader per node
     */
    private static final int MAX_JOB_COUNT = 1;

    /**
     * Default interval to acquire a lease from coordination store
     */
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2_000;

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

    private final Buffer<Record<Event>> buffer;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final MongoDBSourceConfig sourceConfig;
    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final Counter successPartitionCounter;
    private final Counter failureParitionCounter;
    private final AtomicInteger activeExportPartitionConsumerGauge;

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 10;

    private final RecordBufferWriter recordBufferWriter;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final ExecutorService executor;


    public ExportWorker(final EnhancedSourceCoordinator sourceCoordinator,
                        final Buffer<Record<Event>> buffer,
                        final PluginMetrics pluginMetrics,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final MongoDBSourceConfig sourceConfig) {
        this.sourceCoordinator = sourceCoordinator;
        this.buffer = buffer;
        executor = Executors.newFixedThreadPool(MAX_JOB_COUNT);
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        final RecordConverter recordConverter = new RecordConverter(sourceConfig.getCollections().get(0));
        recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator, sourceConfig.getCollections().get(0),
                recordConverter, pluginMetrics, Instant.now().toEpochMilli());
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
        this.startLine = 0;// replace it with checkpoint line
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.successPartitionCounter = pluginMetrics.counter(SUCCESS_PARTITION_COUNTER_NAME);
        this.failureParitionCounter = pluginMetrics.counter(FAILURE_PARTITION_COUNTER_NAME);
        this.activeExportPartitionConsumerGauge = pluginMetrics.gauge(ACTIVE_EXPORT_PARTITION_CONSUMERS_GAUGE, numOfWorkers);
    }

    @Override
    public void run() {
        LOG.info("Starting Export worker to process partitions for export");

        while (!Thread.currentThread().isInterrupted()) {
            DataQueryPartition dataQueryPartition = null;
            try {
                if (numOfWorkers.get() < MAX_JOB_COUNT) {
                    final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE);

                    if (sourcePartition.isPresent()) {
                        dataQueryPartition = (DataQueryPartition) sourcePartition.get();
                        final Optional<AcknowledgementSet> acknowledgementSet = createAcknowledgementSet(dataQueryPartition);
                        processDataQueryPartition(dataQueryPartition);
                        updateState(dataQueryPartition.getCollection(), dataQueryPartition.getProgressState().get().getLoadedRecords());
                        // After global state is updated, mask the partition as completed.
                        sourceCoordinator.completePartition(dataQueryPartition);
                        // TODO add check pointer
                        acknowledgementSet.ifPresent(AcknowledgementSet::complete);
                    }
                }
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The ExportWorker was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception while processing an export data partition, backing off and retrying", e);
                sourceCoordinator.giveUpPartition(dataQueryPartition);
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The ExportWorker was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
        LOG.warn("ExportWorker is interrupted, stopping all data partition loaders...");
        // Cannot call executor.shutdownNow() here
        // Otherwise the final checkpoint will fail due to SDK interruption.
        executor.shutdown();
    }


    private void processDataQueryPartition(final DataQueryPartition partition) {
        final List<String> partitionKeys = List.of(partition.getPartitionKey().split(PARTITION_KEY_SPLITTER));
        if (partitionKeys.size() < PARTITION_KEY_PARTS) {
            throw new RuntimeException("Invalid Partition Key. Must as db.collection|gte|lte format. Key: " + partition.getPartitionKey());
        }
        final List<String> collection = List.of(partitionKeys.get(0).split(COLLECTION_SPLITTER));
        final String gte = partitionKeys.get(1);
        final String lte = partitionKeys.get(2);
        final String className = partitionKeys.get(3);
        if (collection.size() < 2) {
            throw new RuntimeException("Invalid Collection Name. Must as db.collection format");
        }
        long lastCheckpointTime = System.currentTimeMillis();
        try (final MongoClient mongoClient = MongoDBConnection.getMongoClient(sourceConfig)) {
            final MongoDatabase db = mongoClient.getDatabase(collection.get(0));
            final MongoCollection<Document> col = db.getCollection(collection.get(1));
            final Bson query = BsonHelper.buildAndQuery(gte, lte, className);
            long totalRecords = 0L;
            long successRecords = 0L;
            long failedRecords = 0L;

            // line count regardless the start line number
            int recordCount = 0;
            int lastRecordNumberProcessed = 0;
            final List<String> records = new ArrayList<>();
            try (MongoCursor<Document> cursor = col.find(query).iterator()) {
                while (cursor.hasNext()) {
                    totalRecords += 1;
                    recordCount += 1;
                    if (totalRecords <= startLine) {
                        continue;
                    }

                    try {
                        final JsonWriterSettings writerSettings = JsonWriterSettings.builder()
                                .outputMode(JsonMode.RELAXED)
                                .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
                                .build();
                        final String record = cursor.next().toJson(writerSettings);
                        records.add(record);

                        if ((recordCount - startLine) % DEFAULT_BATCH_SIZE == 0) {
                            LOG.debug("Write to buffer for line " + (recordCount - DEFAULT_BATCH_SIZE) + " to " + recordCount);
                            recordBufferWriter.writeToBuffer(createAcknowledgementSet(partition).orElse(null), records);
                            records.clear();
                            lastRecordNumberProcessed = recordCount;
                        }

                        if (System.currentTimeMillis() - lastCheckpointTime > DEFAULT_CHECKPOINT_INTERVAL_MILLS) {
                            LOG.debug("Perform regular checkpointing for Data File Loader");
                            // TODO add checkpoint
                            //checkpointer.checkpoint(lastLineProcessed);
                            lastCheckpointTime = System.currentTimeMillis();

                        }

                        successItemsCounter.increment();
                        successRecords += 1;
                    } catch (Exception e) {
                        LOG.error("failed to add record to buffer with error {}", e.getMessage());
                        failureItemsCounter.increment();
                        failedRecords += 1;
                    }
                }

                final Optional<DataQueryProgressState> progressState = partition.getProgressState();
                progressState.get().setLoadedRecords(totalRecords);
                // TODO update progress state
            } catch (Exception e) {
                LOG.error("Exception connecting to cluster {}", e.getMessage());
                throw new RuntimeException(e);
            }

            LOG.info("Records processed: {}, recordCount: {}", totalRecords, recordCount);
        }
    }

    private Optional<AcknowledgementSet> createAcknowledgementSet(final DataQueryPartition partition) {
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            return Optional.of(acknowledgementSetManager.create((result) -> {
                if (result) {
                    completeDataLoader(partition).accept(null, null);
                    LOG.info("Received acknowledgment of completion from sink for data file {}", partition.getPartitionKey());
                } else {
                    LOG.warn("Negative acknowledgment received for data file {}, retrying", partition.getPartitionKey());
                    sourceCoordinator.giveUpPartition(partition);
                }
            }, ACKNOWLEDGEMENT_SET_TIMEOUT)); //sourceConfig.getDataFileAcknowledgmentTimeout());
        }
        return Optional.empty();
    }

    private BiConsumer completeDataLoader(final DataQueryPartition dataQueryPartition) {
        return (v, ex) -> {

            if (!sourceConfig.isAcknowledgmentsEnabled()) {
                numOfWorkers.decrementAndGet();
                if (numOfWorkers.get() == 0) {
                    activeExportPartitionConsumerGauge.decrementAndGet();
                }
            }
            if (ex == null) {
                successPartitionCounter.increment();
                // Update global state
                updateState(dataQueryPartition.getCollection(),
                        dataQueryPartition.getProgressState().get().getLoadedRecords());
                // After global state is updated, mask the partition as completed.
                sourceCoordinator.completePartition(dataQueryPartition);

            } else {
                // The data loader must have already done one last checkpointing.
                LOG.error("Loading S3 data files completed with an exception: {}", ex);
                // Release the ownership
                sourceCoordinator.giveUpPartition(dataQueryPartition);
            }
        };
    }

    /**
     * <p>There is a global state with sourcePartitionKey the export Arn,
     * to track the number of files are processed. </p>
     * <p>Each time, load of a data file is completed,
     * The state must be updated.</p>
     * Note that the state may be updated since multiple threads are updating the same state.
     * Retry is required.
     *
     * @param collection collection name and database.
     * @param loaded    Number records Loaded.
     */
    private void updateState(final String collection, final long loaded) {

        final String exportPartitionKey = EXPORT_PREFIX + collection;

        // Unlimited retries
        // The state be out of dated when updating.
        while (!Thread.currentThread().isInterrupted()) {
            Optional<EnhancedSourcePartition> globalPartition = sourceCoordinator.getPartition(exportPartitionKey);
            if (globalPartition.isEmpty()) {
                LOG.error("Failed to get load status for " + exportPartitionKey);
                return;
            }

            final GlobalState globalState = (GlobalState) globalPartition.get();
            final LoadStatus loadStatus = LoadStatus.fromMap(globalState.getProgressState().get());
            loadStatus.setLoadedPartitions(loadStatus.getLoadedPartitions() + 1);
            LOG.info("Current status: total {} loaded {}", loadStatus.getTotalPartitions(), loadStatus.getLoadedPartitions());

            loadStatus.setLoadedRecords(loadStatus.getLoadedRecords() + loaded);
            globalState.setProgressState(loadStatus.toMap());

            try {
                sourceCoordinator.saveProgressStateForPartition(globalState, null);
                // if all load are completed.
                if (loadStatus.getLoadedPartitions() == loadStatus.getTotalPartitions()) {
                    LOG.info("All Exports are done, streaming can continue...");
                    sourceCoordinator.createPartition(new GlobalState(STREAM_PREFIX + collection, null));
                }
                break;
            } catch (Exception e) {
                LOG.error("Failed to update the global status, looks like the status was out of date, will retry..");
            }
        }
    }
}
