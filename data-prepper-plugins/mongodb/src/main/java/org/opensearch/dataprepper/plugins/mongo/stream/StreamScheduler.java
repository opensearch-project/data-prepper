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
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class StreamScheduler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamScheduler.class);
    private static final int DEFAULT_TAKE_LEASE_INTERVAL_MILLIS = 60_000;
    static final int DEFAULT_CHECKPOINT_INTERVAL_MILLS = 120_000;
    private static final int DEFAULT_MONITOR_WAIT_TIME_MS = 15_000;
    /**
     * Number of records to accumulate before flushing to buffer
     */
    static final int DEFAULT_BUFFER_BATCH_SIZE = 10;
    /**
     * Number of stream records to accumulate to write to buffer and checkpoint
     */
    static final int DEFAULT_STREAM_BATCH_SIZE = 100;
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
        final RecordConverter recordConverter = new RecordConverter(sourceConfig.getCollections().get(0), StreamPartition.PARTITION_TYPE);
        recordBufferWriter = ExportRecordBufferWriter.create(bufferAccumulator, sourceConfig.getCollections().get(0),
                recordConverter, pluginMetrics, Instant.now().toEpochMilli());
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            StreamPartition streamPartition = null;
            try {
                final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(StreamPartition.PARTITION_TYPE);
                if (sourcePartition.isPresent()) {
                    streamPartition = (StreamPartition) sourcePartition.get();
                    final DataStreamPartitionCheckpoint partitionCheckpoint = new DataStreamPartitionCheckpoint(sourceCoordinator, streamPartition);
                    final StreamAcknowledgementManager streamAcknowledgementManager = new StreamAcknowledgementManager(acknowledgementSetManager, partitionCheckpoint,
                            sourceConfig.getPartitionAcknowledgmentTimeout(), DEFAULT_MONITOR_WAIT_TIME_MS, DEFAULT_CHECKPOINT_INTERVAL_MILLS);
                    final StreamWorker streamWorker = StreamWorker.create(recordBufferWriter, sourceConfig,
                            streamAcknowledgementManager, partitionCheckpoint, pluginMetrics, DEFAULT_STREAM_BATCH_SIZE, DEFAULT_CHECKPOINT_INTERVAL_MILLS);
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
                try {
                    Thread.sleep(DEFAULT_TAKE_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException ex) {
                    LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                    break;
                }
            }
        }
    }
}
