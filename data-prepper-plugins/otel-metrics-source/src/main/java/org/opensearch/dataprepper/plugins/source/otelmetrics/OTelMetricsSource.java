/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.throttling.ThrottlingService;

import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.http.LogThrottlingStrategy;
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
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.HealthGrpcService;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.plugins.source.otelmetrics.http.ArmeriaHttpService;
import org.opensearch.dataprepper.plugins.source.otelmetrics.http.HttpExceptionHandler;
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
import java.util.function.Function;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;

@DataPrepperPlugin(name = "otel_metrics_source", pluginType = Source.class, pluginConfigurationType = OTelMetricsSourceConfig.class)
public class OTelMetricsSource implements Source<Record<? extends Metric>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsSource.class);
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelMetricsSourceConfig oTelMetricsSourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final CertificateProviderFactory certificateProviderFactory;
    private final PluginFactory pluginFactory;
    private Server server;
    private final ByteDecoder byteDecoder;

    private static final int MAX_PENDING_REQUESTS = 1024;
    private static final int BUFFER_WRITE_TIMEOUT_IN_MS = 100;
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    static final String REGEX_HEALTH = "regex:^/(?!health$).*$";

    @DataPrepperPluginConstructor
    public OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig, final PluginMetrics pluginMetrics,
                             final PluginFactory pluginFactory, final PipelineDescription pipelineDescription) {
        this(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelMetricsSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig,
                      final PluginMetrics pluginMetrics,
                      final PluginFactory pluginFactory,
                      final CertificateProviderFactory certificateProviderFactory,
                      final PipelineDescription pipelineDescription) {
        oTelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelMetricsSourceConfig = oTelMetricsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.pluginFactory = pluginFactory;
        this.byteDecoder = new OTelMetricDecoder(oTelMetricsSourceConfig.getOutputFormat());
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
            final ServerBuilder serverBuilder = Server.builder();

            configureHeadersAndHealthCheck(serverBuilder);
            configureTLS(serverBuilder);
            configureAuthentication(serverBuilder);
            final ScheduledThreadPoolExecutor executor = configureTaskExecutor(serverBuilder);

            configureGrpcService(serverBuilder, buffer);
            if (oTelMetricsSourceConfig.getHttpPath() != null) {
                configureHttpService(serverBuilder, buffer, executor.getQueue());
            }

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
        LOG.info("Started otel_metrics_source...");
    }

    private void configureHeadersAndHealthCheck(ServerBuilder serverBuilder) {
        serverBuilder.disableServerHeader();

        if ((oTelMetricsSourceConfig.isEnableUnframedRequests() || oTelMetricsSourceConfig.getHttpPath() != null)
                && oTelMetricsSourceConfig.isHealthCheck()) {
            LOG.info("HTTP source health check is enabled for metrics source");
            serverBuilder.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }

        serverBuilder.maxNumConnections(oTelMetricsSourceConfig.getMaxConnectionCount());
        serverBuilder.requestTimeout(Duration.ofMillis(oTelMetricsSourceConfig.getRequestTimeoutInMillis()));

        if (oTelMetricsSourceConfig.getMaxRequestLength() != null) {
            serverBuilder.maxRequestLength(oTelMetricsSourceConfig.getMaxRequestLength().getBytes());
        }
    }

    private void configureTLS(ServerBuilder serverBuilder) {
        if (oTelMetricsSourceConfig.isSsl()) {
            LOG.info("Creating metrics http source with SSL/TLS enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            // TODO: enable encrypted key with password
            serverBuilder.https(oTelMetricsSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
        } else {
            LOG.warn("Creating otlp metrics source without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the otlp metrics source, go here: https://docs.opensearch.org/latest/data-prepper/pipelines/configuration/sources/otel-metrics-source/#ssl");
            serverBuilder.http(oTelMetricsSourceConfig.getPort());
        }
    }

    private void configureAuthentication(ServerBuilder serverBuilder) {
        if (oTelMetricsSourceConfig.getAuthentication() != null) {
            final Optional<Function<? super HttpService, ? extends HttpService>> optionalAuthDecorator =
                    createHttpAuthentication()
                            .flatMap(ArmeriaHttpAuthenticationProvider::getAuthenticationDecorator);

            if (oTelMetricsSourceConfig.isUnauthenticatedHealthCheck()) {
                optionalAuthDecorator.ifPresent(authDecorator ->
                        serverBuilder.decorator(REGEX_HEALTH, authDecorator));
            } else {
                optionalAuthDecorator.ifPresent(serverBuilder::decorator);
            }
        }
    }

    private ScheduledThreadPoolExecutor configureTaskExecutor(ServerBuilder serverBuilder) {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(oTelMetricsSourceConfig.getThreadCount());
        serverBuilder.blockingTaskExecutor(executor, true);
        return executor;
    }

    private Optional<ArmeriaHttpAuthenticationProvider> createHttpAuthentication() {
        if (oTelMetricsSourceConfig.getAuthentication() == null || oTelMetricsSourceConfig.getAuthentication().getPluginName().equals(UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel_metrics_source http service without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-metrics-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-metrics-source#authentication-configurations");
            return Optional.empty();
        } else {
            return Optional.of(createHttpAuthenticationProvider(oTelMetricsSourceConfig.getAuthentication()));
        }
    }

    private ArmeriaHttpAuthenticationProvider createHttpAuthenticationProvider(final PluginModel authenticationConfiguration) {
        Map<String, Object> pluginSettings = authenticationConfiguration.getPluginSettings();
        return pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, new PluginSetting(authenticationConfiguration.getPluginName(), pluginSettings));
    }

    private GrpcAuthenticationProvider createGrpcAuthenticationProvider(final PluginFactory pluginFactory) {
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

    private void configureHttpService(ServerBuilder serverBuilder, Buffer<Record<? extends Metric>> buffer, BlockingQueue<Runnable> blockingQueue) {
        final String path = oTelMetricsSourceConfig.getHttpPath().replace("${pipelineName}", pipelineName);
        LOG.info("Configuring HTTP service under {} ", path);

        final ArmeriaHttpService armeriaHttpService = new ArmeriaHttpService(buffer, pluginMetrics, BUFFER_WRITE_TIMEOUT_IN_MS, createOtelProtoDecoder());
        final RetryInfoConfig retryInfo = oTelMetricsSourceConfig.getRetryInfo() != null ? oTelMetricsSourceConfig.getRetryInfo() : new RetryInfoConfig();
        final HttpExceptionHandler httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());

        final int maxPendingRequests = MAX_PENDING_REQUESTS;
        final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(maxPendingRequests, blockingQueue);
        final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests, pluginMetrics);
        serverBuilder.decorator(path, ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));

        if (CompressionOption.NONE.equals(oTelMetricsSourceConfig.getCompression())) {
            serverBuilder.annotatedService(path, armeriaHttpService, httpExceptionHandler);
        } else {
            serverBuilder.annotatedService(
                    path,
                    armeriaHttpService,
                    DecodingService.newDecorator(),
                    httpExceptionHandler);
        }

    }

    private OTelProtoCodec.OTelProtoDecoder createOtelProtoDecoder() {
        return oTelMetricsSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder();
    }

    private void configureGrpcService(ServerBuilder serverBuilder, Buffer<Record<? extends Metric>> buffer) {
        LOG.info("Configuring gRPC metrics service");

        final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                .builder()
                .useClientTimeoutHeader(false)
                .useBlockingTaskExecutor(true)
                .exceptionHandler(createGrpcExceptionHandler(oTelMetricsSourceConfig));
        final OTelMetricsGrpcService oTelmetricsGrpcService = new OTelMetricsGrpcService(
                (int) (oTelMetricsSourceConfig.getRequestTimeoutInMillis() * 0.8),
                createOtelProtoDecoder(),
                buffer,
                oTelMetricsSourceConfig.getBufferPartitionKeys(),
                pluginMetrics,
                null
        );
        GrpcAuthenticationProvider authProvider = createGrpcAuthenticationProvider(pluginFactory);

        final List<ServerInterceptor> interceptors = new ArrayList<>();
        if (authProvider.getAuthenticationInterceptor() != null) {
            interceptors.add(authProvider.getAuthenticationInterceptor());
        }

        if (oTelMetricsSourceConfig.isEnableUnframedRequests()) {
            grpcServiceBuilder.enableUnframedRequests(true);
        }

        final CreateServer.GRPCServiceConfig<?, ?> grpcServiceConfig = new CreateServer.GRPCServiceConfig<>(oTelmetricsGrpcService);
        if (oTelMetricsSourceConfig.getPath() != null) {
            final String path = oTelMetricsSourceConfig.getPath().replace("${pipelineName}", pipelineName);
            LOG.info("custom gRPC metrics path: {} ", path);
            grpcServiceBuilder.addService(
                    path,
                    ServerInterceptors.intercept(grpcServiceConfig.getService(), interceptors),
                    MetricsServiceGrpc.getExportMethod());
        } else {
            grpcServiceBuilder.addService(ServerInterceptors.intercept(grpcServiceConfig.getService(), interceptors));
        }

        if (oTelMetricsSourceConfig.isHealthCheck()) {
            LOG.info("Health check for gRPC metrics service is enabled");
            grpcServiceBuilder.addService(new HealthGrpcService());
        }

        if (oTelMetricsSourceConfig.isProtoReflectionService()) {
            LOG.info("Proto reflection service for metrics source is enabled");
            grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
        }

        if (CompressionOption.NONE.equals(oTelMetricsSourceConfig.getCompression())) {
            serverBuilder.service(grpcServiceBuilder.build());
        } else {
            serverBuilder.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
        }
    }

    private GrpcExceptionHandlerFunction createGrpcExceptionHandler(OTelMetricsSourceConfig config) {
        RetryInfoConfig retryInfo = config.getRetryInfo() != null
                ? config.getRetryInfo()
                : new RetryInfoConfig();

        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
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
}
