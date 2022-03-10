/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import com.amazon.dataprepper.plugins.health.HealthGrpcService;
import com.amazon.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@DataPrepperPlugin(name = "otel_metrics_source", pluginType = Source.class)
public class OTelMetricsSource implements Source<Record<ExportMetricsServiceRequest>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsSource.class);
    private final OTelMetricSourceConfig oTelMetricSourceConfig;
    private Server server;
    private final PluginMetrics pluginMetrics;
    private final CertificateProviderFactory certificateProviderFactory;

    public OTelMetricsSource(final PluginSetting pluginSetting) {
        oTelMetricSourceConfig = OTelMetricSourceConfig.buildConfig(pluginSetting);
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        certificateProviderFactory = new CertificateProviderFactory(oTelMetricSourceConfig);
    }

    // accessible only in the same package for unit test
    OTelMetricsSource(final PluginSetting pluginSetting, final CertificateProviderFactory certificateProviderFactory) {
        oTelMetricSourceConfig = OTelMetricSourceConfig.buildConfig(pluginSetting);
        pluginMetrics = PluginMetrics.fromPluginSetting(pluginSetting);
        this.certificateProviderFactory = certificateProviderFactory;
    }

    @Override
    public void start(Buffer<Record<ExportMetricsServiceRequest>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {
            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .addService(new OTelMetricsGrpcService(
                            oTelMetricSourceConfig.getRequestTimeoutInMillis(),
                            buffer,
                            pluginMetrics
                    ))
                    .useClientTimeoutHeader(false);

            if (oTelMetricSourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelMetricSourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            grpcServiceBuilder.enableUnframedRequests(oTelMetricSourceConfig.enableUnframedRequests());

            final ServerBuilder sb = Server.builder();
            sb.service(grpcServiceBuilder.build());
            sb.requestTimeoutMillis(oTelMetricSourceConfig.getRequestTimeoutInMillis());

            // ACM Cert for SSL takes preference
            if (oTelMetricSourceConfig.isSsl() || oTelMetricSourceConfig.useAcmCertForSSL()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(oTelMetricSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
                );
            } else {
                sb.http(oTelMetricSourceConfig.getPort());
            }

            sb.maxNumConnections(oTelMetricSourceConfig.getMaxConnectionCount());
            sb.blockingTaskExecutor(
                    Executors.newScheduledThreadPool(oTelMetricSourceConfig.getThreadCount()),
                    true);

            LOG.info("Metrics Source started");
            server = sb.build();
        }
        try {
            server.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Started otel_metrics_source...");
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                } else {
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        LOG.info("Stopped otel_metrics_source.");
    }

    public OTelMetricSourceConfig getoTelMetricSourceConfig() {
        return oTelMetricSourceConfig;
    }
}
