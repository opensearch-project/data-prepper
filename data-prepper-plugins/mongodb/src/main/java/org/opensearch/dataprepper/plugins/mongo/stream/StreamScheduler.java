package org.opensearch.dataprepper.plugins.mongo.stream;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.PartitionKeyRecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);
    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 120_000;
    static final int DEFAULT_BUFFER_WRITE_INTERVAL_MILLS = 15_000;
    private static final int DEFAULT_MONITOR_WAIT_TIME_MS = 15_000;
    /**
     * Number of records to accumulate before flushing to buffer
     */
    static final int DEFAULT_BUFFER_BATCH_SIZE = 10;
    /**
     * Number of stream records to accumulate to write to buffer and checkpoint
     */
    static final int DEFAULT_RECORD_FLUSH_BATCH_SIZE = 100;
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RecordBufferWriter recordBufferWriter;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final MongoDBSourceConfig sourceConfig;
    private final PluginMetrics pluginMetrics;
    public StreamScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final Buffer<Record<Event>> buffer,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final MongoDBSourceConfig sourceConfig,
                           final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator, pluginMetrics);
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public void run() {
        StreamPartition streamPartition = null;
        while (!Thread.currentThread().isInterrupted()) {
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    streamPartition = (StreamPartition) sourcePartition.get();
                    final StreamWorker streamWorker = getStreamWorker(streamPartition);
                    streamWorker.processStream(streamPartition);
                }
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            } catch (final Exception e) {
                LOG.error("Received an exception during stream processing from DocumentDB, backing off and retrying", e);
                if (streamPartition != null) {
                    sourceCoordinator.giveUpPartition(streamPartition);
                }

                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
    }

    private StreamWorker getStreamWorker (final StreamPartition streamPartition) {
        final DataStreamPartitionCheckpoint partitionCheckpoint = new DataStreamPartitionCheckpoint(sourceCoordinator, streamPartition);
        final StreamAcknowledgementManager streamAcknowledgementManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint,
                sourceConfig.getPartitionAcknowledgmentTimeout(), DEFAULT_MONITOR_WAIT_TIME_MS, DEFAULT_CHECKPOINT_INTERVAL_MILLS);
        final PartitionKeyRecordConverter recordConverter = new PartitionKeyRecordConverter(streamPartition.getCollection(),StreamPartition.PARTITION_TYPE);
        final CollectionConfig partitionCollectionConfig = sourceConfig.getCollections().stream()
                .filter(collectionConfig -> collectionConfig.getCollection().equals(streamPartition.getCollection()))
                .findFirst()
                .get();
        return StreamWorker.create(recordBufferWriter, recordConverter, sourceConfig,
                streamAcknowledgementManager, partitionCheckpoint, pluginMetrics, DEFAULT_RECORD_FLUSH_BATCH_SIZE,
                DEFAULT_CHECKPOINT_INTERVAL_MILLS, DEFAULT_BUFFER_WRITE_INTERVAL_MILLS, partitionCollectionConfig.getStreamBatchSize());
    }
}
