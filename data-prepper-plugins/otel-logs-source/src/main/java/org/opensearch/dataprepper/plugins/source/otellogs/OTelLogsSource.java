/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.source.otellogs.grpc.GrpcServiceConfigurator;
import org.opensearch.dataprepper.plugins.source.otellogs.http.HttpExceptionHandler;
import org.opensearch.dataprepper.plugins.source.otellogs.http.HttpServiceConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@DataPrepperPlugin(name = "otlp_logs",
        deprecatedName = "otel_logs_source",
        pluginType = Source.class, pluginConfigurationType = OTelLogsSourceConfig.class)
public class OTelLogsSource implements Source<Record<Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsSource.class);
    private static final String PLUGIN_NAME = "otlp_logs";
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelLogsSourceConfig oTelLogsSourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ByteDecoder byteDecoder;
    final Counter requestsTooLargeCounter;
    private Server server;

    @DataPrepperPluginConstructor
    public OTelLogsSource(final OTelLogsSourceConfig oTelLogsSourceConfig,
                          final PluginMetrics pluginMetrics,
                          final PluginFactory pluginFactory,
                          final PipelineDescription pipelineDescription) {
        this(oTelLogsSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelLogsSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelLogsSource(final OTelLogsSourceConfig oTelLogsSourceConfig, final PluginMetrics pluginMetrics,
                   final PluginFactory pluginFactory,
                   final CertificateProviderFactory certificateProviderFactory,
                   final PipelineDescription pipelineDescription) {
        oTelLogsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelLogsSourceConfig = oTelLogsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.authenticationProvider = createAuthenticationProvider();
        this.byteDecoder = new OTelLogsDecoder(oTelLogsSourceConfig.getOutputFormat());
        this.requestsTooLargeCounter = pluginMetrics.counter(HttpExceptionHandler.REQUESTS_TOO_LARGE);
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
        LOG.info("Started {} on port {}...", PLUGIN_NAME, oTelLogsSourceConfig.getPort());
    }

    @Override
    public void stop() {
        stopServer();
        LOG.info("Stopped {}.", PLUGIN_NAME);
    }

    private Server buildServer(final Buffer<Record<Object>> buffer) {
        final ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(oTelLogsSourceConfig.getThreadCount());

        final ServerBuilder sb = Server.builder();
        sb.disableServerHeader();
        configureServerLimits(sb);
        configureTls(sb);
        configureTaskExecutor(sb, executor);
        configureHttpHealthCheck(sb);

        final OTelLogsGrpcService grpcService = new OTelLogsGrpcService(
                (int) (oTelLogsSourceConfig.getRequestTimeoutInMillis() * 0.8),
                createOtelProtoDecoder(),
                buffer,
                pluginMetrics,
                null);

        new GrpcServiceConfigurator(oTelLogsSourceConfig, pluginMetrics, pipelineName, authenticationProvider)
                .configure(sb, grpcService);

        if (oTelLogsSourceConfig.getHttpPath() != null) {
            new HttpServiceConfigurator(oTelLogsSourceConfig, pluginMetrics, pipelineName, pluginFactory)
                    .configure(sb, buffer, executor.getQueue());
        }

        return sb.build();
    }

    private void configureServerLimits(final ServerBuilder sb) {
        sb.requestTimeoutMillis(oTelLogsSourceConfig.getRequestTimeoutInMillis());
        sb.maxNumConnections(oTelLogsSourceConfig.getMaxConnectionCount());
        if (oTelLogsSourceConfig.getMaxRequestLength() != null) {
            sb.maxRequestLength(oTelLogsSourceConfig.getMaxRequestLength().getBytes());
        }
        sb.accessLogWriter(log -> {
            if (log.responseCause() instanceof ContentTooLargeException) {
                requestsTooLargeCounter.increment();
            }
        }, true);
    }

    private void configureTls(final ServerBuilder sb) {
        if (oTelLogsSourceConfig.isSsl() || oTelLogsSourceConfig.useAcmCertForSSL()) {
            LOG.info("SSL/TLS is enabled.");
            final Certificate certificate = certificateProviderFactory.getCertificateProvider().getCertificate();
            sb.https(oTelLogsSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
        } else {
            LOG.warn("Creating {} without SSL/TLS. This is not secure.", PLUGIN_NAME);
            LOG.warn("In order to set up TLS for the {}, go here: https://docs.opensearch.org/latest/data-prepper/pipelines/configuration/sources/otel-logs-source/#ssl", PLUGIN_NAME);
            sb.http(oTelLogsSourceConfig.getPort());
        }
    }

    private void configureTaskExecutor(final ServerBuilder sb, final ScheduledThreadPoolExecutor executor) {
        sb.blockingTaskExecutor(executor, true);
    }

    private void configureHttpHealthCheck(final ServerBuilder sb) {
        if ((oTelLogsSourceConfig.enableUnframedRequests() || oTelLogsSourceConfig.getHttpPath() != null) && oTelLogsSourceConfig.hasHealthCheck()) {
            LOG.info("HTTP health check is enabled.");
            sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }
    }

    private OTelProtoCodec.OTelProtoDecoder createOtelProtoDecoder() {
        return oTelLogsSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH
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
        final PluginModel authConfig = oTelLogsSourceConfig.getAuthentication();
        if (authConfig == null || authConfig.getPluginName()
                .equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating {} without authentication. This is not secure.", PLUGIN_NAME);
            LOG.warn("In order to set up Http Basic authentication for the {}, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-logs-source#authentication-configurations", PLUGIN_NAME);
        }
        final PluginSetting setting = authConfig != null
                ? new PluginSetting(authConfig.getPluginName(), authConfig.getPluginSettings())
                : new PluginSetting(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        setting.setPipelineName(pipelineName);
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, setting);
    }
}
