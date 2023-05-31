/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.aws.sqs.common.BufferAccumulator;
import org.opensearch.dataprepper.plugins.aws.sqs.common.codec.Codec;
import org.opensearch.dataprepper.plugins.aws.sqs.common.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.aws.sqs.common.metrics.SqsMetrics;

@DataPrepperPlugin(name = "sqs", pluginType = Source.class,pluginConfigurationType = SqsSourceConfig.class)
public class SqsSource implements Source<Record<Event>> {

    private final SqsSourceConfig sqsSourceConfig;

    private final AcknowledgementSetManager acknowledgementSetManager;

    private final PluginMetrics pluginMetrics;

    private SqsSourceService sqsSourceService;

    private final boolean acknowledgementsEnabled;

    private final Codec codec;

    @DataPrepperPluginConstructor
    public SqsSource(final PluginMetrics pluginMetrics,
                     final SqsSourceConfig sqsSourceConfig,
                     final AcknowledgementSetManager acknowledgementSetManager,
                     final PluginFactory pluginFactory) {
        this.sqsSourceConfig = sqsSourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.acknowledgementsEnabled = sqsSourceConfig.getAcknowledgements();
        this.pluginMetrics = pluginMetrics;
        codec = new JsonCodec();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer is null");
        }
        final BufferAccumulator<Record<Event>> bufferAccumulator =
                BufferAccumulator.create(buffer, sqsSourceConfig.getNumberOfRecordsToAccumulate(),
                        sqsSourceConfig.getBufferTimeout());

        sqsSourceService = new SqsSourceService(sqsSourceConfig,
                acknowledgementSetManager,
                new SqsMetrics(pluginMetrics),
                bufferAccumulator,
                codec);
        sqsSourceService.processSqsMessages();
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return acknowledgementsEnabled;
    }

    @Override
    public void stop() {
        sqsSourceService.stop();
    }
}
