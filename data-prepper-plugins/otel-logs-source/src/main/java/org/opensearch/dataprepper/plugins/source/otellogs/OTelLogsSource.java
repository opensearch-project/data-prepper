/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;

import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
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
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.HealthGrpcService;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.source.otellogs.http.ArmeriaHttpService;
import org.opensearch.dataprepper.plugins.source.otellogs.http.HttpExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;

@DataPrepperPlugin(name = "otel_logs_source", pluginType = Source.class, pluginConfigurationType = OTelLogsSourceConfig.class)
public class OTelLogsSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsSource.class);
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelLogsSourceConfig oTelLogsSourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ByteDecoder byteDecoder;
    private final PluginFactory pluginFactory;
    private Server server;

    @DataPrepperPluginConstructor
    public OTelLogsSource(final OTelLogsSourceConfig oTelLogsSourceConfig,
                          final PluginMetrics pluginMetrics,
                          final PluginFactory pluginFactory,
                          final PipelineDescription pipelineDescription) {
        this(oTelLogsSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelLogsSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelLogsSource(final OTelLogsSourceConfig oTelLogsSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                   final CertificateProviderFactory certificateProviderFactory, final PipelineDescription pipelineDescription) {
        oTelLogsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelLogsSourceConfig = oTelLogsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.pluginFactory = pluginFactory;
        this.byteDecoder = new OTelLogsDecoder(oTelLogsSourceConfig.getOutputFormat());
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
        final SessionProtocol protocol = oTelLogsSourceConfig.isSsl() ? SessionProtocol.HTTPS : SessionProtocol.HTTP;
        final ServerBuilder serverBuilder = Server.builder().port(oTelLogsSourceConfig.getPort(), protocol);
        if (server == null) {
            configureServer(serverBuilder);

            final String transformedGrpcPath = oTelLogsSourceConfig.getPath().replace("${pipelineName}", pipelineName);
            configureGrpcService(serverBuilder, buffer, transformedGrpcPath);

            final String transformedHttpPath = oTelLogsSourceConfig.getHttpPath().replace("${pipelineName}", pipelineName);
            configureHttpService(serverBuilder, buffer, transformedHttpPath);

            server = serverBuilder.build();
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
        LOG.info("Started otel_logs_source...");
    }

    private void configureServer(ServerBuilder serverBuilder) {
        serverBuilder.disableServerHeader();
        if (oTelLogsSourceConfig.isSsl()) {
            LOG.info("Creating http source with SSL/TLS enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            // TODO: enable encrypted key with password
            serverBuilder.https(oTelLogsSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
        } else {
            LOG.warn("Creating otlp logs source without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the otlp logs source, go here: https://docs.opensearch.org/latest/data-prepper/pipelines/configuration/sources/otel-logs-source/#ssl");
            serverBuilder.http(oTelLogsSourceConfig.getPort());
        }

        if (oTelLogsSourceConfig.getAuthentication() != null) {
            createHttpAuthentication()
                    .flatMap(ArmeriaHttpAuthenticationProvider::getAuthenticationDecorator)
                    .ifPresent(serverBuilder::decorator);
        }

        serverBuilder.maxNumConnections(oTelLogsSourceConfig.getMaxConnectionCount());
        serverBuilder.requestTimeout(Duration.ofMillis(oTelLogsSourceConfig.getRequestTimeoutInMillis()));

        if (oTelLogsSourceConfig.getMaxRequestLength() != null) {
            serverBuilder.maxRequestLength(oTelLogsSourceConfig.getMaxRequestLength().getBytes());
        }
        final int threadCount = oTelLogsSourceConfig.getThreadCount();
        serverBuilder.blockingTaskExecutor(new ScheduledThreadPoolExecutor(threadCount), true);

        if (oTelLogsSourceConfig.hasHealthCheck()) {
            LOG.info("HTTP source health check is enabled");
            serverBuilder.service("/health", HealthCheckService.builder().longPolling(0).build());
        }
    }

    private void configureHttpService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer, String path) {
        final ArmeriaHttpService armeriaHttpService = new ArmeriaHttpService(buffer, pluginMetrics, 100);
        final RetryInfoConfig retryInfo = oTelLogsSourceConfig.getRetryInfo();
        final HttpExceptionHandler httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
        if (CompressionOption.NONE.equals(oTelLogsSourceConfig.getCompression())) {
            serverBuilder.annotatedService(path, armeriaHttpService, httpExceptionHandler);
        } else {
            serverBuilder.annotatedService(
                    path,
                    armeriaHttpService,
                    DecodingService.newDecorator(),
                    httpExceptionHandler);
        }
    }

    private void configureGrpcService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer,  String path) {
        final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                .builder()
                .useClientTimeoutHeader(false)
                .useBlockingTaskExecutor(true)
                .exceptionHandler(createGrpExceptionHandler(oTelLogsSourceConfig));
        final OTelLogsGrpcService oTelLogsGrpcService = new OTelLogsGrpcService(
                (int) (oTelLogsSourceConfig.getRequestTimeoutInMillis() * 0.8),
                oTelLogsSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
                buffer,
                pluginMetrics,
                null
        );
        GrpcAuthenticationProvider authProvider = createGrpcAuthenticationProvider(pluginFactory);

        final List<ServerInterceptor> interceptors = new ArrayList<>();
        if (authProvider.getAuthenticationInterceptor() != null) {
            interceptors.add(authProvider.getAuthenticationInterceptor());
        }

        if (oTelLogsSourceConfig.enableUnframedRequests()) {
            grpcServiceBuilder.enableUnframedRequests(true);
        }

        final CreateServer.GRPCServiceConfig<?, ?> grpcServiceConfig = new CreateServer.GRPCServiceConfig<>(oTelLogsGrpcService);
        grpcServiceBuilder.addService(
                path,
                ServerInterceptors.intercept(grpcServiceConfig.getService(), interceptors),
                LogsServiceGrpc.getExportMethod());

        if (oTelLogsSourceConfig.hasHealthCheck()) {
            LOG.info("Health check is enabled");
            grpcServiceBuilder.addService(new HealthGrpcService());
        }

        if (oTelLogsSourceConfig.hasProtoReflectionService()) {
            LOG.info("Proto reflection service is enabled");
            grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
        }

        if (CompressionOption.NONE.equals(oTelLogsSourceConfig.getCompression())) {
            serverBuilder.service(grpcServiceBuilder.build());
        } else {
            serverBuilder.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
        }
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
        LOG.info("Stopped otel_logs_source.");
    }

    private GrpcAuthenticationProvider createGrpcAuthenticationProvider(final PluginFactory pluginFactory) {
        final PluginModel authenticationConfiguration = oTelLogsSourceConfig.getAuthentication();

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel-logs-source without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-logs-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-logs-source#authentication-configurations");
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

    private Optional<ArmeriaHttpAuthenticationProvider> createHttpAuthentication() {
        if (oTelLogsSourceConfig.getAuthentication() == null || oTelLogsSourceConfig.getAuthentication().getPluginName().equals(UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel_trace_source http service without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-trace-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations");
            return Optional.empty();
        } else {
            return Optional.of(createGrpcAuthenticationProvider(oTelLogsSourceConfig.getAuthentication()));
        }
    }

    private ArmeriaHttpAuthenticationProvider createGrpcAuthenticationProvider(final PluginModel authenticationConfiguration) {
        Map<String, Object> pluginSettings = authenticationConfiguration.getPluginSettings();
        return pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, new PluginSetting(authenticationConfiguration.getPluginName(), pluginSettings));
    }

    private GrpcExceptionHandlerFunction createGrpExceptionHandler(OTelLogsSourceConfig config) {
        RetryInfoConfig retryInfo = config.getRetryInfo() != null
                ? config.getRetryInfo()
                : new RetryInfoConfig();

        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
    }
}
