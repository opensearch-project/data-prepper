/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.plugins.sink.otlp.buffer.OtlpSinkBuffer;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 *  OTLP Sink Plugin for Data Prepper.
 *  Supports traces, metrics, and logs.
 */
@Experimental
@DataPrepperPlugin(
        name = "otlp",
        pluginType = Sink.class,
        pluginConfigurationType = OtlpSinkConfig.class
)
public class OtlpSink extends AbstractSink<Record<Event>> {
    private volatile boolean initialized = false;

    private final OtlpSinkBuffer buffer;
    private final OtlpSinkMetrics sinkMetrics;

    /**
     * Constructor for the OTLP sink plugin.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config        the configuration for the sink
     * @param pluginMetrics the plugin metrics to use
     * @param pluginSetting the plugin setting to use
     */
    @DataPrepperPluginConstructor
    public OtlpSink(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final OtlpSinkConfig config, @Nonnull final PluginMetrics pluginMetrics, @Nonnull final PluginSetting pluginSetting) {
        super(pluginSetting);

        this.sinkMetrics = new OtlpSinkMetrics(pluginMetrics, pluginSetting);
        this.buffer = new OtlpSinkBuffer(awsCredentialsSupplier, config, sinkMetrics);
    }

    /**
     * Initialize the buffer
     */
    @Override
    public void doInitialize() {
        buffer.start();
        initialized = true;
    }

    /**
     * Implement the sink's output logic
     *
     * @param records Records to be output (can be Span, Metric, or Log events)
     */
    @Override
    public void doOutput(@Nonnull final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            buffer.add(record);
        }
    }

    /**
     * Indicates whether this sink is ready to receive data.
     *
     * @return true if the sink is ready
     */
    @Override
    public boolean isReady() {
        return initialized && buffer.isRunning();
    }

    /**
     * Hook called during pipeline shutdown.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        buffer.stop();
    }
}
