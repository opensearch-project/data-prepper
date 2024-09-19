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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisSourceConfig;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.KinesisStreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KinesisShardRecordProcessorFactoryTest {
    private KinesisShardRecordProcessorFactory kinesisShardRecordProcessorFactory;

    private static final String streamId = "stream-1";
    private static final String codec_plugin_name = "json";

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    StreamIdentifier streamIdentifier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private KinesisSourceConfig kinesisSourceConfig;

    @Mock
    private KinesisStreamConfig kinesisStreamConfig;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private InputCodec codec;

    @BeforeEach
    void setup() {
        MockitoAnnotations.initMocks(this);

        PluginModel pluginModel = mock(PluginModel.class);
        when(pluginModel.getPluginName()).thenReturn(codec_plugin_name);
        when(pluginModel.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(kinesisSourceConfig.getCodec()).thenReturn(pluginModel);
        when(kinesisSourceConfig.getNumberOfRecordsToAccumulate()).thenReturn(100);

        codec = mock(InputCodec.class);
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any())).thenReturn(codec);

        when(streamIdentifier.streamName()).thenReturn(streamId);
        when(kinesisStreamConfig.getName()).thenReturn(streamId);
        when(kinesisSourceConfig.getStreams()).thenReturn(List.of(kinesisStreamConfig));
    }

    @Test
    void testKinesisRecordProcessFactoryReturnsKinesisRecordProcessor() {
        kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec);
        assertInstanceOf(KinesisRecordProcessor.class, kinesisShardRecordProcessorFactory.shardRecordProcessor(streamIdentifier));
    }

    @Test
    void testKinesisRecordProcessFactoryDefaultUnsupported() {
        kinesisShardRecordProcessorFactory = new KinesisShardRecordProcessorFactory(buffer, kinesisSourceConfig, acknowledgementSetManager, pluginMetrics, codec);
        assertThrows(UnsupportedOperationException.class, () -> kinesisShardRecordProcessorFactory.shardRecordProcessor());
    }
}
