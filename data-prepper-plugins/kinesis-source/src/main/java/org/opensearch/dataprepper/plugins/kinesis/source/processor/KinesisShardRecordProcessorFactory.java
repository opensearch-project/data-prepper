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

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.converter.KinesisRecordConverter;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Buffer<Record<Event>> buffer;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginMetrics pluginMetrics;
    private final KinesisRecordConverter kinesisRecordConverter;

    public KinesisShardRecordProcessorFactory(Buffer<Record<Event>> buffer,
                                              KinesisSourceConfig kinesisSourceConfig,
                                              final AcknowledgementSetManager acknowledgementSetManager,
                                              final PluginMetrics pluginMetrics,
                                              final InputCodec codec) {
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.buffer = buffer;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.kinesisRecordConverter = new KinesisRecordConverter(codec);
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        throw new UnsupportedOperationException("Use the method with stream details!");
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
        BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer,
                kinesisSourceConfig.getNumberOfRecordsToAccumulate(), kinesisSourceConfig.getBufferTimeout());
        return new KinesisRecordProcessor(bufferAccumulator, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, kinesisRecordConverter, streamIdentifier);
    }
}