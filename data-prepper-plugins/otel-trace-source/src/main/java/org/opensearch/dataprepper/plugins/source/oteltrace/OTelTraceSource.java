/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.linecorp.armeria.server.Server;
import io.grpc.MethodDescriptor;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelTraceDecoder;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "otel_trace_source", pluginType = Source.class, pluginConfigurationType = OTelTraceSourceConfig.class)
public class OTelTraceSource implements Source<Record<Object>> {
    private static final String PLUGIN_NAME = "otel_trace_source";
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private final String pipelineName;
    private Server server;
    private final ByteDecoder byteDecoder;

    @DataPrepperPluginConstructor
    public OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                           final PipelineDescription pipelineDescription) {
        this(oTelTraceSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelTraceSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                    final CertificateProviderFactory certificateProviderFactory, final PipelineDescription pipelineDescription) {
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
        this.byteDecoder = new OTelTraceDecoder(oTelTraceSourceConfig.getOutputFormat());
    }

    @Override
    public ByteDecoder getDecoder() {
        return byteDecoder;
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {

            final OTelTraceGrpcService oTelTraceGrpcService = new OTelTraceGrpcService(
                    (int)(oTelTraceSourceConfig.getRequestTimeoutInMillis() * 0.8),
                    oTelTraceSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
                    buffer,
                    pluginMetrics
            );

            ServerConfiguration serverConfiguration = ConvertConfiguration.convertConfiguration(oTelTraceSourceConfig);
            CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, PLUGIN_NAME, pipelineName);
            CertificateProvider certificateProvider = null;
            if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) {
                certificateProvider = certificateProviderFactory.getCertificateProvider();
            }
            final MethodDescriptor<ExportTraceServiceRequest, ExportTraceServiceResponse> methodDescriptor = TraceServiceGrpc.getExportMethod();
            server = createServer.createGRPCServer(authenticationProvider, oTelTraceGrpcService, certificateProvider, methodDescriptor);

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
        LOG.info("Started otel_trace_source on port " + oTelTraceSourceConfig.getPort() + "...");
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
        LOG.info("Stopped otel_trace_source.");
    }

    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory) {
        final PluginModel authenticationConfiguration = oTelTraceSourceConfig.getAuthentication();

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel-trace-source without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-trace-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations");
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
