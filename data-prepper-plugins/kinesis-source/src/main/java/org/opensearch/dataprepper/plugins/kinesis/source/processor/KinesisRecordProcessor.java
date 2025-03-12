/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.KinesisRecordConverter;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisStreamNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Duration;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class KinesisRecordProcessor implements ShardRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordProcessor.class);

    private static final int DEFAULT_MONITOR_WAIT_TIME_MS = 15_000;
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofSeconds(20);

    private final StreamIdentifier streamIdentifier;
    private final KinesisStreamConfig kinesisStreamConfig;
    private final Duration checkpointInterval;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final KinesisRecordConverter kinesisRecordConverter;
    private final KinesisCheckpointerTracker kinesisCheckpointerTracker;
    private final ExecutorService executorService;
    private String kinesisShardId;
    private long lastCheckpointTimeInMillis;
    private final int bufferTimeoutMillis;
    private final AcknowledgementSetManager acknowledgementSetManager;

    private final Counter acknowledgementSetSuccesses;
    private final Counter acknowledgementSetFailures;
    private final Counter recordsProcessed;
    private final Counter recordProcessingErrors;
    private final Counter checkpointFailures;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    public static final String ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME = "acknowledgementSetSuccesses";
    public static final String ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME = "acknowledgementSetFailures";
    public static final String KINESIS_RECORD_PROCESSED_METRIC_NAME = "recordProcessed";
    public static final String KINESIS_RECORD_PROCESSING_ERRORS_METRIC_NAME = "recordProcessingErrors";
    public static final String KINESIS_RECORD_BYTES_RECEIVED_METRIC_NAME = "bytesReceived";
    public static final String KINESIS_RECORD_BYTES_PROCESSED_METRIC_NAME = "bytesProcessed";
    public static final String KINESIS_CHECKPOINT_FAILURES = "checkpointFailures";
    public static final String KINESIS_STREAM_TAG_KEY = "stream";
    private AtomicBoolean isStopRequested;

    public KinesisRecordProcessor(final BufferAccumulator<Record<Event>> bufferAccumulator,
                                  final KinesisSourceConfig kinesisSourceConfig,
                                  final AcknowledgementSetManager acknowledgementSetManager,
                                  final PluginMetrics pluginMetrics,
                                  final KinesisRecordConverter kinesisRecordConverter,
                                  final KinesisCheckpointerTracker kinesisCheckpointerTracker,
                                  final StreamIdentifier streamIdentifier) {
        this.bufferTimeoutMillis = (int) kinesisSourceConfig.getBufferTimeout().toMillis();
        this.streamIdentifier = streamIdentifier;
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.kinesisStreamConfig = getStreamConfig(kinesisSourceConfig);
        this.kinesisRecordConverter = kinesisRecordConverter;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementSetSuccesses = pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_SUCCESS_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.acknowledgementSetFailures = pluginMetrics.counterWithTags(ACKNOWLEDGEMENT_SET_FAILURES_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.recordsProcessed = pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSED_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.recordProcessingErrors = pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSING_ERRORS_METRIC_NAME, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.checkpointFailures = pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.bytesReceivedSummary = pluginMetrics.summary(KINESIS_RECORD_BYTES_RECEIVED_METRIC_NAME);
        this.bytesProcessedSummary = pluginMetrics.summary(KINESIS_RECORD_BYTES_PROCESSED_METRIC_NAME);
        this.checkpointInterval = kinesisStreamConfig.getCheckPointInterval();
        this.bufferAccumulator = bufferAccumulator;
        this.kinesisCheckpointerTracker = kinesisCheckpointerTracker;
        this.executorService = Executors.newSingleThreadExecutor(BackgroundThreadFactory.defaultExecutorThreadFactory("kinesis-ack-monitor"));
        this.isStopRequested = new AtomicBoolean(false);
    }

    private KinesisStreamConfig getStreamConfig(final KinesisSourceConfig kinesisSourceConfig) {
        final Optional<KinesisStreamConfig> kinesisStreamConfig = kinesisSourceConfig.getStreams().stream().filter(streamConfig -> streamConfig.getName().equals(streamIdentifier.streamName())).findAny();
        if (kinesisStreamConfig.isEmpty()) {
            throw new KinesisStreamNotFoundException(String.format("Kinesis stream not found for %s", streamIdentifier.streamName()));
        }
        return kinesisStreamConfig.get();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        // Called once when the processor is initialized.
        kinesisShardId = initializationInput.shardId();
        String kinesisStreamName = streamIdentifier.streamName();
        LOG.info("Initialize Processor for stream: {},  shard: {}", kinesisStreamName, kinesisShardId);
        lastCheckpointTimeInMillis = System.currentTimeMillis();

        if (kinesisSourceConfig.isAcknowledgments()) {
            executorService.submit(() -> monitorCheckpoint(executorService));
        }
    }

    private void monitorCheckpoint(final ExecutorService executorService) {
        while (!isStopRequested.get()) {
            if (System.currentTimeMillis() - lastCheckpointTimeInMillis >= checkpointInterval.toMillis()) {
                doCheckpoint();
            }
            try {
                Thread.sleep(DEFAULT_MONITOR_WAIT_TIME_MS);
            } catch (InterruptedException ex) {
                break;
            }
        }
        executorService.shutdown();
    }

    private AcknowledgementSet createAcknowledgmentSet(final ProcessRecordsInput processRecordsInput,
                                                       final ExtendedSequenceNumber extendedSequenceNumber) {
        return acknowledgementSetManager.create((result) -> {
            String kinesisStreamName = streamIdentifier.streamName();
            if (result) {
                acknowledgementSetSuccesses.increment();
                kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(extendedSequenceNumber);
                LOG.debug("acknowledgements received for stream: {}, shardId: {}", kinesisStreamName, kinesisShardId);
            } else {
                acknowledgementSetFailures.increment();
                LOG.debug("acknowledgements received with false for stream: {}, shardId: {}", kinesisStreamName, kinesisShardId);
            }
        }, ACKNOWLEDGEMENT_SET_TIMEOUT);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            Optional<AcknowledgementSet> acknowledgementSetOpt = Optional.empty();
            boolean acknowledgementsEnabled = kinesisSourceConfig.isAcknowledgments();
            ExtendedSequenceNumber extendedSequenceNumber = getLatestSequenceNumberFromInput(processRecordsInput);
            if (acknowledgementsEnabled) {
                acknowledgementSetOpt = Optional.of(createAcknowledgmentSet(processRecordsInput, extendedSequenceNumber));
            }

            // Track the records for checkpoint purpose
            kinesisCheckpointerTracker.addRecordForCheckpoint(extendedSequenceNumber, processRecordsInput.checkpointer());

            List<KinesisInputOutputRecord> kinesisOutputRecords = kinesisRecordConverter.convert(
                    kinesisStreamConfig.getCompression().getDecompressionEngine(),
                    processRecordsInput.records(), streamIdentifier.streamName());

            int eventCount = 0;
            for (KinesisInputOutputRecord kinesisInputOutputRecord: kinesisOutputRecords) {
                Record<Event> dataPrepperRecord = kinesisInputOutputRecord.getDataPrepperRecord();
                long incomingRecordSizeBytes = kinesisInputOutputRecord.getIncomingRecordSizeBytes();
                bytesReceivedSummary.record(incomingRecordSizeBytes);
                Event event = dataPrepperRecord.getData();
                acknowledgementSetOpt.ifPresent(acknowledgementSet -> acknowledgementSet.add(event));

                bufferAccumulator.add(dataPrepperRecord);
                bytesProcessedSummary.record(incomingRecordSizeBytes);
                eventCount++;
            }

            // Flush buffer at the end
            bufferAccumulator.flush();
            recordsProcessed.increment(eventCount);

            // If acks are not enabled, mark the sequence number for checkpoint
            if (!acknowledgementsEnabled) {
                kinesisCheckpointerTracker.markSequenceNumberForCheckpoint(extendedSequenceNumber);
            }

            LOG.debug("Number of Records {} written for stream: {}, shardId: {}", eventCount, streamIdentifier.streamName(), kinesisShardId);

            acknowledgementSetOpt.ifPresent(AcknowledgementSet::complete);

            // Checkpoint for shard
            if (!acknowledgementsEnabled && (System.currentTimeMillis() - lastCheckpointTimeInMillis >= checkpointInterval.toMillis())) {
                doCheckpoint();
            }
        } catch (Exception ex) {
            recordProcessingErrors.increment();
            LOG.error("Failed writing shard data to buffer: ", ex);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        LOG.debug("Lease Lost");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        String kinesisStream = streamIdentifier.streamName();
        LOG.debug("Reached shard end, checkpointing for stream: {}, shardId: {}", kinesisStream, kinesisShardId);
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        String kinesisStream = streamIdentifier.streamName();
        isStopRequested.set(true);
        LOG.debug("Scheduler is shutting down, checkpointing for stream: {}, shardId: {}", kinesisStream, kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    @VisibleForTesting
    public void checkpoint(RecordProcessorCheckpointer checkpointer, String sequenceNumber, long subSequenceNumber) {
        try {
            String kinesisStream = streamIdentifier.streamName();
            LOG.debug("Checkpoint for stream: {}, shardId: {}, sequence: {}, subsequence: {}", kinesisStream, kinesisShardId, sequenceNumber, subSequenceNumber);
            checkpointer.checkpoint(sequenceNumber, subSequenceNumber);
        } catch (ShutdownException | ThrottlingException | InvalidStateException ex) {
            LOG.debug("Caught exception at checkpoint, skipping checkpoint.", ex);
            checkpointFailures.increment();
        }
    }

    private void doCheckpoint() {
        LOG.debug("Regular checkpointing for shard {}", kinesisShardId);
        Optional<KinesisCheckpointerRecord> kinesisCheckpointerRecordOptional = kinesisCheckpointerTracker.popLatestReadyToCheckpointRecord();
        if (kinesisCheckpointerRecordOptional.isPresent()) {
            ExtendedSequenceNumber lastExtendedSequenceNumber = kinesisCheckpointerRecordOptional.get().getExtendedSequenceNumber();
            RecordProcessorCheckpointer recordProcessorCheckpointer = kinesisCheckpointerRecordOptional.get().getCheckpointer();
            checkpoint(recordProcessorCheckpointer, lastExtendedSequenceNumber.sequenceNumber(), lastExtendedSequenceNumber.subSequenceNumber());
            lastCheckpointTimeInMillis = System.currentTimeMillis();
        }
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        try {
            String kinesisStream = streamIdentifier.streamName();
            LOG.debug("Checkpoint for stream: {}, shardId: {}", kinesisStream, kinesisShardId);
            checkpointer.checkpoint();
        } catch (ShutdownException | ThrottlingException | InvalidStateException ex) {
            LOG.debug("Caught exception at checkpoint, skipping checkpoint.", ex);
            checkpointFailures.increment();
        }
    }

    private ExtendedSequenceNumber getLatestSequenceNumberFromInput(final ProcessRecordsInput processRecordsInput) {
        ListIterator<KinesisClientRecord> recordIterator = processRecordsInput.records().listIterator();
        ExtendedSequenceNumber largestExtendedSequenceNumber = null;
        while (recordIterator.hasNext()) {
            KinesisClientRecord record = recordIterator.next();
            ExtendedSequenceNumber extendedSequenceNumber =
                    new ExtendedSequenceNumber(record.sequenceNumber(), record.subSequenceNumber());

            if (largestExtendedSequenceNumber == null
                    || largestExtendedSequenceNumber.compareTo(extendedSequenceNumber) < 0) {
                largestExtendedSequenceNumber = extendedSequenceNumber;
            }
        }
        return largestExtendedSequenceNumber;
    }
}
