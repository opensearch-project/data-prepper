package org.opensearch.dataprepper.plugins.kinesis.source.processor;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Buffer<Record<Event>> buffer;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;

    public KinesisShardRecordProcessorFactory(Buffer<Record<Event>> buffer,
                                              KinesisSourceConfig kinesisSourceConfig,
                                              final AcknowledgementSetManager acknowledgementSetManager,
                                              final PluginMetrics pluginMetrics,
                                              final PluginFactory pluginFactory) {
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.buffer = buffer;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        throw new UnsupportedOperationException("Use the method with stream details!");
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
        return new KinesisRecordProcessor(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, pluginFactory, streamIdentifier);
    }
}