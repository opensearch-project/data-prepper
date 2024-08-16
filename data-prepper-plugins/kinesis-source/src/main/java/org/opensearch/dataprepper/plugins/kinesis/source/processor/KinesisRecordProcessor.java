package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.KinesisSource;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class KinesisRecordProcessor implements ShardRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);
    private final StreamIdentifier streamIdentifier;
    private final KinesisStreamConfig kinesisStreamConfig;
    private final int checkpointIntervalMilliSeconds;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final Buffer<Record<Event>> buffer;
    private String kinesisShardId;
    private final InputCodec codec;
    private long lastCheckpointTimeInMillis;
    private final int bufferTimeoutMillis;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Counter acknowledgementSetCallbackCounter;
    private final Counter recordProcessingErrors;
    private final Counter checkpointFailures;
    private static final Duration ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofSeconds(20);
    private static final String ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME = "acknowledgementSetCallbackCounter";
    public static final String KINESIS_RECORD_PROCESSING_ERRORS = "recordProcessingErrors";
    public static final String KINESIS_CHECKPOINT_FAILURES = "checkpointFailures";
    public static final String KINESIS_STREAM_TAG_KEY = "stream";

    public KinesisRecordProcessor(Buffer<Record<Event>> buffer,
                                  final KinesisSourceConfig kinesisSourceConfig,
                                  final AcknowledgementSetManager acknowledgementSetManager,
                                  final PluginMetrics pluginMetrics,
                                  final PluginFactory pluginFactory,
                                  final StreamIdentifier streamIdentifier) {
        this.bufferTimeoutMillis = (int) kinesisSourceConfig.getBufferTimeout().toMillis();
        this.streamIdentifier = streamIdentifier;
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.kinesisStreamConfig = getStreamConfig(kinesisSourceConfig);
        final PluginModel codecConfiguration = kinesisSourceConfig.getCodec();
        final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
        this.codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementSetCallbackCounter = pluginMetrics.counter(ACKNOWLEDGEMENT_SET_CALLBACK_METRIC_NAME);
        this.recordProcessingErrors = pluginMetrics.counterWithTags(KINESIS_RECORD_PROCESSING_ERRORS, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.checkpointFailures = pluginMetrics.counterWithTags(KINESIS_CHECKPOINT_FAILURES, KINESIS_STREAM_TAG_KEY, streamIdentifier.streamName());
        this.checkpointIntervalMilliSeconds = kinesisStreamConfig.getCheckPointIntervalInMilliseconds();
        this.buffer = buffer;
    }

    private KinesisStreamConfig getStreamConfig(final KinesisSourceConfig kinesisSourceConfig) {
        return kinesisSourceConfig.getStreams().stream().filter(streamConfig -> streamConfig.getName().equals(streamIdentifier.streamName())).findAny().get();
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        // Called once when the processor is initialized.
        kinesisShardId = initializationInput.shardId();
        LOG.info("Initialize Processor for shard: " + kinesisShardId);
        lastCheckpointTimeInMillis = System.currentTimeMillis();
    }

    private AcknowledgementSet createAcknowledgmentSet(final ProcessRecordsInput processRecordsInput) {
        return acknowledgementSetManager.create((result) -> {
            acknowledgementSetCallbackCounter.increment();
            if (result) {
                LOG.info("acknowledgements received");
                checkpoint(processRecordsInput.checkpointer());
            } else {
                LOG.info("acknowledgements received with false");
            }

        }, ACKNOWLEDGEMENT_SET_TIMEOUT);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<Record<Event>> records = new ArrayList<>();

        try {
            AcknowledgementSet acknowledgementSet;
            boolean acknowledgementsEnabled = kinesisSourceConfig.isAcknowledgments();
            if (acknowledgementsEnabled) {
                acknowledgementSet = createAcknowledgmentSet(processRecordsInput);
            } else {
                acknowledgementSet = null;
            }

            for (KinesisClientRecord record : processRecordsInput.records()) {
                processRecord(record, records::add);
            }

            if (acknowledgementSet != null) {
                records.forEach(record -> {
                    acknowledgementSet.add(record.getData());
                });
            }

            buffer.writeAll(records, bufferTimeoutMillis);

            if (acknowledgementSet != null) {
                acknowledgementSet.complete();
            }

            // Checkpoint for shard
            if (kinesisStreamConfig.isEnableCheckPoint() && System.currentTimeMillis() - lastCheckpointTimeInMillis > checkpointIntervalMilliSeconds) {
                LOG.info("Regular checkpointing for shard " + kinesisShardId);
                checkpoint(processRecordsInput.checkpointer());
                lastCheckpointTimeInMillis = System.currentTimeMillis();
            }
        } catch (Exception ex) {
            recordProcessingErrors.increment();
            LOG.error("Failed writing shard data to buffer: ", ex);
        }
    }

    private void processRecord(KinesisClientRecord record, Consumer<Record<Event>> eventConsumer) throws IOException {
        // Read bytebuffer
        byte[] arr = new byte[record.data().remaining()];
        record.data().get(arr);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(arr);
        codec.parse(byteArrayInputStream, eventConsumer);
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        LOG.debug("Lease Lost");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        LOG.info("Reached shard end, checkpointing shard: {}", kinesisShardId);
        checkpoint(shardEndedInput.checkpointer());
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        LOG.info("Scheduler is shutting down, checkpointing shard: {}", kinesisShardId);
        checkpoint(shutdownRequestedInput.checkpointer());
    }

    private void checkpoint(RecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (ShutdownException | ThrottlingException | InvalidStateException ex) {
            LOG.info("Caught exception at checkpoint, skipping checkpoint.", ex);
            checkpointFailures.increment();
        }
    }
}
