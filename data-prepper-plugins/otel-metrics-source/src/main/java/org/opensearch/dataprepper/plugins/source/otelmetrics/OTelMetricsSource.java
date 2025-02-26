/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.linecorp.armeria.server.Server;
import io.grpc.MethodDescriptor;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.opensearch.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "otel_metrics_source", pluginType = Source.class, pluginConfigurationType = OTelMetricsSourceConfig.class)
public class OTelMetricsSource implements Source<Record<? extends Metric>> {
    private static final String PLUGIN_NAME = "otel_metrics_source";
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsSource.class);
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelMetricsSourceConfig oTelMetricsSourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private Server server;
    private final ByteDecoder byteDecoder;

    @DataPrepperPluginConstructor
    public OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig, final PluginMetrics pluginMetrics,
                             final PluginFactory pluginFactory, final PipelineDescription pipelineDescription) {
        this(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelMetricsSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                      final CertificateProviderFactory certificateProviderFactory, final PipelineDescription pipelineDescription) {
        oTelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelMetricsSourceConfig = oTelMetricsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
        this.byteDecoder = new OTelMetricDecoder();
    }

    @Override
    public ByteDecoder getDecoder() {
        return byteDecoder;
    }

    @Override
    public void start(Buffer<Record<? extends Metric>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {
            final OTelMetricsGrpcService oTelMetricsGrpcService = new OTelMetricsGrpcService(
                    (int) (oTelMetricsSourceConfig.getRequestTimeoutInMillis() * 0.8),
                    new OTelProtoCodec.OTelProtoDecoder(),
                    buffer,
                    pluginMetrics
            );

            ServerConfiguration serverConfiguration = ConvertConfiguration.convertConfiguration(oTelMetricsSourceConfig);
            CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, PLUGIN_NAME, pipelineName);
            CertificateProvider certificateProvider = null;
            if (oTelMetricsSourceConfig.isSsl() || oTelMetricsSourceConfig.useAcmCertForSSL()) {
                certificateProvider = certificateProviderFactory.getCertificateProvider();
            }
            final MethodDescriptor<ExportMetricsServiceRequest, ExportMetricsServiceResponse> methodDescriptor = MetricsServiceGrpc.getExportMethod();
            server = createServer.createGRPCServer(authenticationProvider, oTelMetricsGrpcService, certificateProvider, methodDescriptor);

            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
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


    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory) {
        final PluginModel authenticationConfiguration = oTelMetricsSourceConfig.getAuthentication();

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel-metrics-source without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-metrics-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-metrics-source#authentication-configurations");
        }

        final PluginSetting authenticationPluginSetting;
        if (authenticationConfiguration != null) {
            authenticationPluginSetting = new PluginSetting(authenticationConfiguration.getPluginName(), authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting = new PluginSetting(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, authenticationPluginSetting);
    }
}
