/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.neptune.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.source.neptune.client.NeptuneStreamClient;
import org.opensearch.dataprepper.plugins.source.neptune.client.NeptuneStreamEventListener;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.neptune.converter.NeptuneStreamRecordValidator;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.model.S3PartitionStatus;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamCheckpoint;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.StreamPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.neptunedata.model.ExpiredStreamException;
import software.amazon.awssdk.services.neptunedata.model.InvalidParameterException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StreamWorker implements NeptuneStreamEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(StreamWorker.class);

    static final String SUCCESS_ITEM_COUNTER_NAME = "changeEventsProcessed";
    static final String FAILURE_ITEM_COUNTER_NAME = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    private static final long BUFFER_WRITE_TIMEOUT_MILLIS = Duration.ofSeconds(15).toMillis();
    private static final long S3_PARTITIONS_WAIT_TIME_MILLIS = Duration.ofSeconds(10).toMillis();

    private static final int STREAM_RECORDS_BATCH_SIZE = 10_000;

    private final NeptuneSourceConfig sourceConfig;
    private final RecordBufferWriter recordBufferWriter;
    private final StreamRecordConverter streamRecordConverter;
    private final DataStreamPartitionCheckpoint partitionCheckpoint;

    private final Counter successItemsCounter;
    private final Counter failureItemsCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    private final NeptuneStreamRecordValidator streamRecordValidator;
    private final NeptuneStreamClient streamClient;
    private final StreamAcknowledgementManager streamAcknowledgementManager;
    private final PluginMetrics pluginMetrics;

    private final int recordFlushBatchSize;
    private final int checkPointIntervalInMs;
    private final int bufferWriteIntervalInMs;
    private final int streamBatchSize;

    private boolean stopWorker = false;
    private boolean isUnrecoverableError = false;

    private final ExecutorService streamCheckpointExecutorService;

    S3PartitionStatus s3PartitionStatus;

    final List<Event> records;
    long lastBufferWriteTime = System.currentTimeMillis();

    private StreamCheckpoint lastLocalCheckpoint;
    private final StreamCheckpoint currentCheckpoint;

    private final Lock lock;

    public static StreamWorker create(final RecordBufferWriter recordBufferWriter,
                                      final StreamRecordConverter recordConverter,
                                      final NeptuneSourceConfig sourceConfig,
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
                        final StreamRecordConverter streamRecordConverter,
                        final NeptuneSourceConfig sourceConfig,
                        final StreamAcknowledgementManager streamAcknowledgementManager,
                        final DataStreamPartitionCheckpoint partitionCheckpoint,
                        final PluginMetrics pluginMetrics,
                        final int recordFlushBatchSize,
                        final int checkPointIntervalInMs,
                        final int bufferWriteIntervalInMs,
                        final int streamBatchSize
    ) {
        this.recordBufferWriter = recordBufferWriter;
        this.streamRecordConverter = streamRecordConverter;
        this.sourceConfig = sourceConfig;
        this.streamAcknowledgementManager = streamAcknowledgementManager;
        this.streamRecordValidator = new NeptuneStreamRecordValidator(sourceConfig.isEnableNonStringIndexing());
        this.partitionCheckpoint = partitionCheckpoint;
        this.pluginMetrics = pluginMetrics;
        this.recordFlushBatchSize = recordFlushBatchSize;
        this.checkPointIntervalInMs = checkPointIntervalInMs;
        this.bufferWriteIntervalInMs = bufferWriteIntervalInMs;
        this.streamBatchSize = streamBatchSize;
        this.successItemsCounter = pluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME);
        this.failureItemsCounter = pluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.lock = new ReentrantLock();
        this.lastLocalCheckpoint = this.currentCheckpoint = new StreamCheckpoint(new StreamPosition(0L, 0L), 0L);
        this.streamClient = new NeptuneStreamClient(sourceConfig, STREAM_RECORDS_BATCH_SIZE, this);
        this.records = new ArrayList<>();
        // this.documentDBAggregateMetrics = documentDBAggregateMetrics;

        if (sourceConfig.isAcknowledgments()) {
            streamAcknowledgementManager.init((Void) -> stop());
        }

        // buffer write and checkpoint in separate thread on timeout
        // TODO:: can probably a scheduled executor
        this.streamCheckpointExecutorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("neptune-stream-checkpoint"));
        this.streamCheckpointExecutorService.submit(this::bufferWriteAndCheckpointStream);
    }

    private boolean shouldWaitForS3Partition() {
        final Optional<S3PartitionStatus> globalS3FolderCreationStatus = partitionCheckpoint.getGlobalS3FolderCreationStatus();
        if (globalS3FolderCreationStatus.isPresent()) {
            s3PartitionStatus = globalS3FolderCreationStatus.get();
            return false;
        }
        return true;
    }

    private void initializeS3Partitions() {
        while (shouldWaitForS3Partition() && !Thread.currentThread().isInterrupted()) {
            LOG.info("S3 partitions are not ready, waiting for them to be complete before resuming streams.");
            try {
                Thread.sleep(S3_PARTITIONS_WAIT_TIME_MILLIS);
            } catch (final InterruptedException ex) {
                LOG.info("The StreamScheduler was interrupted while waiting to retry, stopping processing");
                Thread.currentThread().interrupt();
                break;
            }
        }

        final List<String> s3Partitions = s3PartitionStatus.getPartitions();
        if (s3Partitions.isEmpty()) {
            // This should not happen unless the S3 partition creator failed.
            // documentDBAggregateMetrics.getStream5xxErrors().increment();
            throw new IllegalStateException("S3 partitions are not created. Please check the S3 partition creator thread.");
        }
        streamRecordConverter.setPartitions(s3Partitions);

    }

    public void processStream(final StreamPartition streamPartition)  {
        // documentDBAggregateMetrics.getStreamApiInvocations().increment();

        try {
            initializeS3Partitions();

            LOG.info("Starting to watch streams for change events.");

            setCheckpointInformation(streamPartition);
            this.streamClient.setStreamPosition(currentCheckpoint.getCommitNum(), currentCheckpoint.getOpNum());

            streamClient.start();

        } catch (final InterruptedException e) {
            LOG.info("StreamWorker thread got interrupted!");
        } catch (final Exception e) {
           LOG.info("Exception encountered with Neptune stream client:", e);
           throw e;
        } finally {
            this.shutdownCleanup();
        }
    }


    private void shutdownCleanup() {

        // Flush remaining records
        if (!records.isEmpty()) {
            LOG.info("Flushing and checkpointing last processed record batch from the stream before terminating");
            flushToBuffer();
        }

        // Do final checkpoint.
        if (!sourceConfig.isAcknowledgments()) {
            partitionCheckpoint.checkpoint(currentCheckpoint);
        }

        stop();

        // kill monitoring thread
        if (this.streamAcknowledgementManager != null) {
            this.streamAcknowledgementManager.shutdown();
        }

        // Stream is invalid, reset the checkpoint before quitting
        if (isUnrecoverableError) {
            partitionCheckpoint.resetCheckpoint();
        }
        partitionCheckpoint.giveUpPartition();
        // stop the checkpointing thread
        this.streamCheckpointExecutorService.shutdownNow();
    }

    private void setCheckpointInformation(final StreamPartition streamPartition) {
        Optional<Long> commitNum = streamPartition.getProgressState().map(StreamProgressState::getCommitNum);
        commitNum.ifPresent(currentCheckpoint::setCommitNum);
        Optional<Long> opNum = streamPartition.getProgressState().map(StreamProgressState::getOpNum);
        opNum.ifPresent(currentCheckpoint::setOpNum);
        Optional<Long> loadedRecords = streamPartition.getProgressState().map(StreamProgressState::getLoadedRecords);
        loadedRecords.ifPresent(currentCheckpoint::setRecordCount);
    }

    private void flushToBuffer(final List<Event> records, final StreamCheckpoint progress) {
        final AcknowledgementSet acknowledgementSet = streamAcknowledgementManager
                .createAcknowledgementSet(new StreamCheckpoint(progress))
                .orElse(null);
        recordBufferWriter.writeToBuffer(acknowledgementSet, records);
        successItemsCounter.increment(records.size());
        if (acknowledgementSet != null) {
            acknowledgementSet.complete();
        }
    }

    private void flushToBuffer() {
        LOG.debug("Write to buffer records [{}-{}]", lastLocalCheckpoint.getRecordCount(), currentCheckpoint.getRecordCount());
        flushToBuffer(records, currentCheckpoint);
        this.lastLocalCheckpoint = new StreamCheckpoint(this.currentCheckpoint);
        lastBufferWriteTime = System.currentTimeMillis();
        records.clear();
    }

    private void bufferWriteAndCheckpointStream() {
        long lastCheckpointTime = System.currentTimeMillis();
        while (!Thread.currentThread().isInterrupted() && !stopWorker) {
            if (!records.isEmpty() && lastBufferWriteTime < Instant.now().minusMillis(BUFFER_WRITE_TIMEOUT_MILLIS).toEpochMilli()) {
                lock.lock();
                LOG.debug("Writing to buffer due to buffer write delay");
                try {
                    flushToBuffer();
                } catch (final Exception e) {
                    // this will only happen if writing to buffer gets interrupted from shutdown,
                    // otherwise it's infinite backoff and retry
                    LOG.error("Failed to add records to buffer with error", e);
                    failureItemsCounter.increment(records.size());
                } finally {
                    lock.unlock();
                }
            }

            if (shouldCheckpoint(lastCheckpointTime)) {
                try {
                    lock.lock();
                    LOG.debug("Perform regular checkpoint for {}", lastLocalCheckpoint);
                    partitionCheckpoint.checkpoint(lastLocalCheckpoint);
                } catch (Exception e) {
                    LOG.warn("Exception checkpointing the current state. The stream record processing will start from previous checkpoint.", e);
                    stop();
                } finally {
                    lock.unlock();
                }
                lastCheckpointTime = System.currentTimeMillis();
            }

            try {
                Thread.sleep(BUFFER_WRITE_TIMEOUT_MILLIS);
            } catch (InterruptedException ex) {
                break;
            }
        }
        LOG.info("Checkpoint monitoring thread interrupted.");
    }

    /**
     * If End-to-End acknowledgements are not enabled then we checkpoint every {@link #checkPointIntervalInMs} ms.
     */
    private boolean shouldCheckpoint(final long lastCheckpointTime) {
        return !sourceConfig.isAcknowledgments() && (System.currentTimeMillis() - lastCheckpointTime >= checkPointIntervalInMs);
    }

    private boolean shouldFlushRecords(final long recordCount) {
        return (recordCount % recordFlushBatchSize == 0) || (System.currentTimeMillis() - lastBufferWriteTime >= bufferWriteIntervalInMs);
    }

    void stop() {
        stopWorker = true;
    }

    @Override
    public void onNeptuneStreamEvents(final List<NeptuneStreamRecord> streamRecords, final StreamPosition streamPosition) {
        for (int i = 0; i < streamRecords.size(); i++) {
            if (!streamRecordValidator.isValid(streamRecords.get(i))) {
                LOG.debug("Skipping record {}.", i);
                continue;
            }
            final Event event = streamRecordConverter.convert(streamRecords.get(i));
            records.add(event);
            // recordBytes.add(bytes);
            lock.lock();
            try {
                currentCheckpoint.setCommitNum(streamRecords.get(i).getCommitNum());
                currentCheckpoint.setOpNum(streamRecords.get(i).getOpNum());
                currentCheckpoint.incrementRecordCount();
                LOG.info("Process stream record - {} ", currentCheckpoint);

                if (shouldFlushRecords(currentCheckpoint.getRecordCount())) {
                    flushToBuffer();
                }
            } catch (Exception e) {
                // this will only happen if writing to buffer gets interrupted from shutdown,
                // otherwise it's infinite backoff and retry
                LOG.error("Failed to add records to buffer with error", e);
                failureItemsCounter.increment(records.size());
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean onNeptuneStreamException(final Exception exception, final StreamPosition streamPosition) {
        if (exception == null || stopWorker) {
            return !stopWorker;
        }

        if (exception instanceof InvalidParameterException || exception instanceof ExpiredStreamException) {
            LOG.warn("Stream is corrupt, stopping the worker and resetting the stream.");
            this.isUnrecoverableError = true;
        } else {
            LOG.info("Error fetching stream data, stopping processing");
        }
        return false;
    }

    @Override
    public boolean shouldStopNeptuneStream(final StreamPosition streamPosition) {
        return stopWorker;
    }
}
