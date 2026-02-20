/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.buffer.OtlpSinkBuffer;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;

/**
 *  OTLP Sink Plugin for Data Prepper.
 *  Supports traces, metrics, and logs with separate buffers per signal type for optimal batching.
 */
@Experimental
@DataPrepperPlugin(
        name = "otlp",
        pluginType = Sink.class,
        pluginConfigurationType = OtlpSinkConfig.class
)
public class OtlpSink extends AbstractSink<Record<Event>> {
    private volatile boolean initialized = false;

    private final Map<OtlpSignalType, OtlpSinkBuffer> buffersBySignalType;
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
        this.buffersBySignalType = createBuffers(awsCredentialsSupplier, config, sinkMetrics);
    }

    private Map<OtlpSignalType, OtlpSinkBuffer> createBuffers(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final OtlpSinkConfig config,
            final OtlpSinkMetrics sinkMetrics) {
        
        final Map<OtlpSignalType, OtlpSinkBuffer> buffers = new EnumMap<>(OtlpSignalType.class);
        final OTelProtoStandardCodec.OTelProtoEncoder encoder = new OTelProtoStandardCodec.OTelProtoEncoder();
        final OtlpHttpSender sender = new OtlpHttpSender(awsCredentialsSupplier, config, sinkMetrics);

        // Create a buffer for each signal type
        for (final OtlpSignalType signalType : new OtlpSignalType[]{OtlpSignalType.TRACE, OtlpSignalType.METRIC, OtlpSignalType.LOG}) {
            final OtlpSignalHandler handler = signalType.createHandler(encoder);
            final OtlpSinkBuffer buffer = new OtlpSinkBuffer(config, sinkMetrics, handler, sender);
            buffers.put(signalType, buffer);
        }

        return buffers;
    }

    /**
     * Initialize the buffers
     */
    @Override
    public void doInitialize() {
        buffersBySignalType.values().forEach(OtlpSinkBuffer::start);
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
            final OtlpSignalType signalType = OtlpSignalType.fromEvent(record.getData());
            final OtlpSinkBuffer buffer = buffersBySignalType.get(signalType);
            
            if (buffer != null) {
                buffer.add(record);
            } else {
                // Unknown signal type, skip
                sinkMetrics.incrementFailedRecordsCount(1);
            }
        }
    }

    /**
     * Indicates whether this sink is ready to receive data.
     *
     * @return true if the sink is ready
     */
    @Override
    public boolean isReady() {
        return initialized && buffersBySignalType.values().stream().allMatch(OtlpSinkBuffer::isRunning);
    }

    /**
     * Hook called during pipeline shutdown.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        buffersBySignalType.values().forEach(OtlpSinkBuffer::stop);
    }
}
