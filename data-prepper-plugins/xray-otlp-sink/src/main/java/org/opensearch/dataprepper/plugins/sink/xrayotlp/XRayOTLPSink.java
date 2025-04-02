/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

/**
 * A Data Prepper Sink plugin that forwards traces to AWS X-Ray's OTLP endpoint.
 */
@DataPrepperPlugin(
        name = "otlp_xray_sink",
        pluginType = Sink.class
)
public class XRayOTLPSink implements Sink<Record<String>> {

    /**
     * Constructs the OTLP X-Ray Sink.
     * Configuration loading will be added in a later iteration.
     */
    public XRayOTLPSink() {
        // TODO: Inject config or plugin setting.
    }

    /**
     * Lifecycle hook invoked during pipeline startup.
     * Initialize AWS clients or other resources here.
     */
    @Override
    public void initialize() {
        // TODO: Initialize AWS X-Ray client
    }

    /**
     * Called each time a batch of records is emitted to the sink.
     * This method is responsible for handling delivery to AWS X-Ray.
     *
     * @param records Collection of OTLP log records to process.
     */
    @Override
    public void output(final Collection<Record<String>> records) {
        // TODO: Process records
    }

    /**
     * Indicates whether this sink is ready to receive data.
     *
     * @return true if the sink is ready
     */
    @Override
    public boolean isReady() {
        // TODO: Implement readiness logic
        return true;
    }

    /**
     * Hook called during pipeline shutdown.
     */
    @Override
    public void shutdown() {
        // TODO: Clean up resources
    }

    /**
     * Updates internal latency metrics using the received records.
     *
     * @param events Collection of records used for latency tracking.
     */
    @Override
    public void updateLatencyMetrics(final Collection<Record<String>> events) {
        // TODO: Implement latency tracking with PluginMetrics
    }
}
