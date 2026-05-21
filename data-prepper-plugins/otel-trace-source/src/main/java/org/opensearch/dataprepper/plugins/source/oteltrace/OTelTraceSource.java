/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
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
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelTraceDecoder;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.source.oteltrace.grpc.GrpcServiceConfigurator;
import org.opensearch.dataprepper.plugins.source.oteltrace.http.HttpServiceConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "otlp_traces",
        deprecatedName = "otel_trace_source",
        pluginType = Source.class, pluginConfigurationType = OTelTraceSourceConfig.class)
public class OTelTraceSource implements Source<Record<Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);
    private static final String PLUGIN_NAME = "otlp_traces";
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private final String pipelineName;
    private final ByteDecoder byteDecoder;
    private Server server;

    @DataPrepperPluginConstructor
    public OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics,
                           final PluginFactory pluginFactory, final PipelineDescription pipelineDescription) {
        this(oTelTraceSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelTraceSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics,
                    final PluginFactory pluginFactory,
                    final CertificateProviderFactory certificateProviderFactory,
                    final PipelineDescription pipelineDescription) {
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.authenticationProvider = createAuthenticationProvider();
        this.byteDecoder = new OTelTraceDecoder(oTelTraceSourceConfig.getOutputFormat());
    }

    @Override
    public ByteDecoder getDecoder() {
        return byteDecoder;
    }

    @Override
    public void start(final Buffer<Record<Object>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            server = buildServer(buffer);
            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
        }
        startServer();
        LOG.info("Started {} on port {}...", PLUGIN_NAME, oTelTraceSourceConfig.getPort());
    }

    @Override
    public void stop() {
        stopServer();
        LOG.info("Stopped {}.", PLUGIN_NAME);
    }

    private Server buildServer(final Buffer<Record<Object>> buffer) {
        final ServerBuilder sb = Server.builder();
        sb.disableServerHeader();
        configureServerLimits(sb);
        configureTls(sb);
        configureTaskExecutor(sb);
        configureHttpHealthCheck(sb);

        final OTelProtoCodec.OTelProtoDecoder decoder = createOtelProtoDecoder();
        final OTelTraceGrpcService grpcService = new OTelTraceGrpcService(
                (int) (oTelTraceSourceConfig.getRequestTimeoutInMillis() * 0.8),
                decoder,
                buffer,
                pluginMetrics,
                null);

        new GrpcServiceConfigurator(oTelTraceSourceConfig, pluginMetrics, pipelineName, authenticationProvider)
                .configure(sb, grpcService);

        if (!oTelTraceSourceConfig.enableUnframedRequests()) {
            new HttpServiceConfigurator(oTelTraceSourceConfig, pluginMetrics, pluginFactory)
                    .configure(sb, decoder, buffer);
        }

        return sb.build();
    }

    private void configureServerLimits(final ServerBuilder sb) {
        sb.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());
        sb.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
        if (oTelTraceSourceConfig.getMaxRequestLength() != null) {
            sb.maxRequestLength(oTelTraceSourceConfig.getMaxRequestLength().getBytes());
        }
    }

    private void configureTls(final ServerBuilder sb) {
        if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) {
            LOG.info("SSL/TLS is enabled.");
            final Certificate certificate = certificateProviderFactory.getCertificateProvider().getCertificate();
            sb.https(oTelTraceSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
        } else {
            LOG.warn("Creating {} without SSL/TLS. This is not secure.", PLUGIN_NAME);
            LOG.warn("In order to set up TLS for the {}, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl", PLUGIN_NAME);
            sb.http(oTelTraceSourceConfig.getPort());
        }
    }

    private void configureTaskExecutor(final ServerBuilder sb) {
        final BlockingTaskExecutor executor = BlockingTaskExecutor.builder()
                .numThreads(oTelTraceSourceConfig.getThreadCount())
                .threadNamePrefix(pipelineName + "-" + PLUGIN_NAME)
                .build();
        sb.blockingTaskExecutor(executor, true);
    }

    private void configureHttpHealthCheck(final ServerBuilder sb) {
        if (oTelTraceSourceConfig.enableHttpHealthCheck()) {
            LOG.info("HTTP health check is enabled.");
            sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }
    }

    private OTelProtoCodec.OTelProtoDecoder createOtelProtoDecoder() {
        return oTelTraceSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH
                ? new OTelProtoOpensearchCodec.OTelProtoDecoder()
                : new OTelProtoStandardCodec.OTelProtoDecoder();
    }

    private void startServer() {
        try {
            server.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            }
            throw new RuntimeException(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private void stopServer() {
        if (server != null) {
            try {
                server.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                }
                throw new RuntimeException(ex);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
    }

    private GrpcAuthenticationProvider createAuthenticationProvider() {
        final PluginModel authConfig = oTelTraceSourceConfig.getAuthentication();
        if (authConfig == null || authConfig.getPluginName()
                .equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating {} without authentication. This is not secure.", PLUGIN_NAME);
            LOG.warn("In order to set up Http Basic authentication for the {}, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations", PLUGIN_NAME);
        }
        final PluginSetting setting = authConfig != null
                ? new PluginSetting(authConfig.getPluginName(), authConfig.getPluginSettings())
                : new PluginSetting(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        setting.setPipelineName(pipelineName);
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, setting);
    }
}
