/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;

import io.grpc.protobuf.services.ProtoReflectionService;

import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsGrpcService;
import org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltelemetry.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@DataPrepperPlugin(name = "otel_telemetry_source", pluginType = Source.class, pluginConfigurationType = OTelTelemetrySourceConfig.class)
public class OTelTelemetrySource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTelemetrySource.class);
    private final OTelTelemetrySourceConfig config;
    private final CertificateProviderFactory certificateProviderFactory;
    private final PluginMetrics pluginMetrics;
    private Server server;

    @DataPrepperPluginConstructor
    public OTelTelemetrySource(final OTelTelemetrySourceConfig config, final PluginMetrics pluginMetrics) {
        this.config = config;
        this.certificateProviderFactory = new CertificateProviderFactory(config);
        this.pluginMetrics = pluginMetrics;
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        @SuppressWarnings("unchecked")
        Buffer<Record<? extends Metric>> metricBuffer = (Buffer<Record<? extends Metric>>) (Object) buffer;
        final ServerBuilder serverBuilder = Server.builder()
                .http(config.getPort())
                .requestTimeoutMillis(config.getRequestTimeout())
                .service("/v1/logs", GrpcService.builder()
                        .addService(ProtoReflectionService.newInstance())
                        .addService(new OTelLogsGrpcService(10000, new OTelProtoStandardCodec.OTelProtoDecoder(),
                        buffer, pluginMetrics))
                        .build())
                .service("/v1/metrics", GrpcService.builder()
                        .addService(ProtoReflectionService.newInstance())
                        .addService(new OTelMetricsGrpcService(10000, new OTelProtoStandardCodec.OTelProtoDecoder(),
                        metricBuffer  , pluginMetrics))  
                        .build())
                .service("/v1/traces", GrpcService.builder()
                        .addService(ProtoReflectionService.newInstance())
                        .addService(new OTelTraceGrpcService(10000, new OTelProtoStandardCodec.OTelProtoDecoder(),
                        buffer, pluginMetrics))
                        .build());

        if (config.isSslEnabled()) {
            LOG.info("SSL/TLS is enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            serverBuilder.https(config.getPort())
                    .tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8))
                    );
        }

        if (config.isHealthCheckServiceEnabled()) {
            serverBuilder.serviceUnder("/health", GrpcService.builder().build());
        }

        server = serverBuilder.build();
        pluginMetrics.gauge("serverConnections", server, Server::numConnections);
        server.start().join();
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop().join();
        }
    }
}
