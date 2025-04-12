/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OtlpTraceOutputCodec;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration.XRayOTLPSinkConfig;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.http.SigV4Signer;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.http.XRayOtlpHttpSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.io.ByteArrayOutputStream;
import java.util.Collection;

/**
 * A Data Prepper Sink plugin that forwards spans to AWS X-Ray OTLP endpoint using
 * OTLP Protobuf encoding and AWS SigV4 authentication.
 */
@DataPrepperPlugin(
        name = "xray_otlp_sink",
        pluginType = Sink.class
)
public class XRayOTLPSink implements Sink<Record<Span>> {

    private static final Logger LOG = LoggerFactory.getLogger(XRayOTLPSink.class);

    private final OutputCodec codec;
    private final XRayOtlpHttpSender httpSender;

    /**
     * Default constructor used by Data Prepper. Initializes codec and HTTP sender
     * using default ApacheHttpClient and basic configuration.
     */
    public XRayOTLPSink() {
        this.codec = new OtlpTraceOutputCodec();
        final XRayOTLPSinkConfig config = new XRayOTLPSinkConfig(); // TODO: Load real config
        final SigV4Signer signer = new SigV4Signer(config);
        this.httpSender = new XRayOtlpHttpSender(signer, ApacheHttpClient.builder().build());
    }

    /**
     * Constructor for unit testing with injected dependencies.
     *
     * @param codec      the OutputCodec to encode spans
     * @param httpSender the HTTP sender to transmit OTLP data
     */
    public XRayOTLPSink(final OutputCodec codec, final XRayOtlpHttpSender httpSender) {
        this.codec = codec;
        this.httpSender = httpSender;
    }

    /**
     * Initializes the sink. Called once during pipeline startup.
     */
    @Override
    public void initialize() {
        // TODO: Initialize AWS X-Ray client
        LOG.info("Initialized XRay OTLP Sink");
    }

    /**
     * Processes a batch of spans and sends them to the AWS X-Ray OTLP endpoint.
     *
     * @param records a collection of span records
     */
    @Override
    public void output(final Collection<Record<Span>> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (final Record<Span> record : records) {
                codec.writeEvent(record.getData(), out);
            }
            httpSender.send(out.toByteArray());
        } catch (final Exception e) {
            LOG.error("Failed to process span records", e);
        }
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
        httpSender.close();
    }

    /**
     * Updates internal latency metrics using the received records.
     *
     * @param events Collection of records used for latency tracking.
     */
    @Override
    public void updateLatencyMetrics(final Collection<Record<Span>> events) {
        // TODO: Implement latency tracking with PluginMetrics
    }
}
