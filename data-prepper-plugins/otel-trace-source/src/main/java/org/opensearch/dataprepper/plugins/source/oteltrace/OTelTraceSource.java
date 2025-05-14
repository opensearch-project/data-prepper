/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.otel.codec.OTelTraceDecoder;
import org.opensearch.dataprepper.plugins.source.oteltrace.grpc.GrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.http.HttpService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "otel_trace_source", pluginType = Source.class, pluginConfigurationType = OTelTraceSourceConfig.class)
public class OTelTraceSource implements Source<Record<Object>> {
    private static final String PLUGIN_NAME = "otel_trace_source";
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);


    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
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
        this.pluginFactory = pluginFactory;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
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
            ServerBuilder serverBuilder = Server.builder().port(oTelTraceSourceConfig.getPort(), inferProtocolFromConfig());

            configureHeadersAndHealthCheck(serverBuilder);
            configureTLS(serverBuilder);
            configureTaskExecutor(serverBuilder);

            configureGrpcService(serverBuilder, buffer);
            // todo tlongo needed until clarified if unframedRequests should survive
            if (!oTelTraceSourceConfig.enableUnframedRequests()) {
                configureHttpService(serverBuilder, buffer);
            }

            server = serverBuilder.build();

            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
        }
        try {
            server.start().get();
        } catch (ExecutionException ex) {
            handleExecutionException(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Started otel_trace_source on port {}...", oTelTraceSourceConfig.getPort());
    }

    private SessionProtocol inferProtocolFromConfig() {
        if (oTelTraceSourceConfig.isSsl()) {
            return SessionProtocol.HTTPS;
        } else {
            return SessionProtocol.HTTP;
        }
    }

    private void handleExecutionException(ExecutionException ex) {
        if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
            throw (RuntimeException) ex.getCause();
        } else {
            throw new RuntimeException(ex);
        }
    }

    private void configureGrpcService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        com.linecorp.armeria.server.grpc.GrpcService grpcService = new GrpcService(pluginFactory, oTelTraceSourceConfig, pluginMetrics, pipelineName).create(buffer, serverBuilder);

        if (CompressionOption.NONE.equals(oTelTraceSourceConfig.getCompression())) {
            serverBuilder.service(grpcService);
        } else {
            serverBuilder.service(grpcService, DecodingService.newDecorator());
        }
    }

    private void configureHttpService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        new HttpService(pluginMetrics, oTelTraceSourceConfig, pluginFactory).create(serverBuilder, buffer);
    }

    private void configureHeadersAndHealthCheck(ServerBuilder serverBuilder) {
        serverBuilder.disableServerHeader();
        if (oTelTraceSourceConfig.enableHttpHealthCheck()) {
            serverBuilder.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }
        serverBuilder.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());
        if(oTelTraceSourceConfig.getMaxRequestLength() != null) {
            serverBuilder.maxRequestLength(oTelTraceSourceConfig.getMaxRequestLength().getBytes());
        }
        serverBuilder.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
    }

    private void configureTLS(ServerBuilder serverBuilder) {
        if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) {
            LOG.info("SSL/TLS is enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            serverBuilder.https(oTelTraceSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
            );
        } else {
            LOG.warn("Creating otel_trace_source without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the otel_trace_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl");
            serverBuilder.http(oTelTraceSourceConfig.getPort());
        }
    }

    private void configureTaskExecutor(ServerBuilder serverBuilder) {
        final BlockingTaskExecutor blockingTaskExecutor = BlockingTaskExecutor.builder()
                .numThreads(oTelTraceSourceConfig.getThreadCount())
                .threadNamePrefix(pipelineName + "-otel_trace")
                .build();
        serverBuilder.blockingTaskExecutor(blockingTaskExecutor, true);
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
}
