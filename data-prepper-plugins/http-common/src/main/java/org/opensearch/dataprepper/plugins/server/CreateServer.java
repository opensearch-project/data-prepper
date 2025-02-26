/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.server;


import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.http.HttpServerConfig;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.http.LogThrottlingStrategy;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;


public class CreateServer {
    private ServerConfiguration serverConfiguration;
    private final Logger LOG;
    private final PluginMetrics  pluginMetrics;
    private String sourceName;
    private String pipelineName;

    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";

    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    public CreateServer(final ServerConfiguration grpcServerConfiguration, final Logger LOG, final PluginMetrics pluginMetrics, final String sourceName, final String pipelineName) {
        this.serverConfiguration = grpcServerConfiguration;
        this.LOG = LOG;
        this.pluginMetrics = pluginMetrics;
        this.sourceName = sourceName;
        this.pipelineName = pipelineName;
    }

    public CreateServer(final Logger LOG, final PluginMetrics pluginMetrics, final String sourceName, final String pipelineName) {
        this.LOG = LOG;
        this.pluginMetrics = pluginMetrics;
        this.sourceName = sourceName;
        this.pipelineName = pipelineName;
    }

    public <K, V> Server createGRPCServer(final GrpcAuthenticationProvider authenticationProvider, final BindableService grpcService, final CertificateProvider certificateProvider, final MethodDescriptor<K, V> methodDescriptor) {
        final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor(authenticationProvider);

        final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                .builder()
                .useClientTimeoutHeader(false)
                .useBlockingTaskExecutor(true)
                .exceptionHandler(createGrpExceptionHandler());

        final String sourcePath = serverConfiguration.getPath();
        if (sourcePath != null) {
            final String transformedSourcePath = sourcePath.replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
            grpcServiceBuilder.addService(transformedSourcePath,
                    ServerInterceptors.intercept(grpcService, serverInterceptors), methodDescriptor);
        } else {
            grpcServiceBuilder.addService(ServerInterceptors.intercept(grpcService, serverInterceptors));
        }

        if (serverConfiguration.hasHealthCheck()) {
            LOG.info("Health check is enabled");
            grpcServiceBuilder.addService(new HealthGrpcService());
        }

        if (serverConfiguration.hasProtoReflectionService()) {
            LOG.info("Proto reflection service is enabled");
            grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
        }

        grpcServiceBuilder.enableUnframedRequests(serverConfiguration.enableUnframedRequests());

        final ServerBuilder sb = Server.builder();
        sb.disableServerHeader();
        if (CompressionOption.NONE.equals(serverConfiguration.getCompression())) {
            sb.service(grpcServiceBuilder.build());
        } else {
            sb.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
        }

        if (serverConfiguration.enableHttpHealthCheck()) {
            sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }

        if(serverConfiguration.getAuthentication() != null) {
            final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService =
                    authenticationProvider.getHttpAuthenticationService();

            if(serverConfiguration.isUnauthenticatedHealthCheck()) {
                optionalHttpAuthenticationService.ifPresent(httpAuthenticationService ->
                        sb.decorator(REGEX_HEALTH, httpAuthenticationService));
            } else {
                optionalHttpAuthenticationService.ifPresent(sb::decorator);
            }
        }

        sb.requestTimeoutMillis(serverConfiguration.getRequestTimeoutInMillis());
        if(serverConfiguration.getMaxRequestLength() != null) {
            sb.maxRequestLength(serverConfiguration.getMaxRequestLength().getBytes());
        }

        // ACM Cert for SSL takes preference
        if (serverConfiguration.isSsl() || serverConfiguration.useAcmCertForSSL()) {
            LOG.info("SSL/TLS is enabled.");
            final Certificate certificate = certificateProvider.getCertificate();
            sb.https(serverConfiguration.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
            );
        } else {
            LOG.warn("Creating " + sourceName + " without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the " + sourceName + ", go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl");
            sb.http(serverConfiguration.getPort());
        }

        final BlockingTaskExecutor blockingTaskExecutor = BlockingTaskExecutor.builder()
                .numThreads(serverConfiguration.getThreadCount())
                .threadNamePrefix(pipelineName + sourceName)
                .build();
        sb.blockingTaskExecutor(blockingTaskExecutor, true);

        return sb.build();
    }

    public Server createHTTPServer(final Buffer<Record<Log>> buffer, final CertificateProviderFactory certificateProviderFactory, final ArmeriaHttpAuthenticationProvider authenticationProvider, final HttpRequestExceptionHandler httpRequestExceptionHandler, final Object logService, final HttpServerConfig httpServerConfig) {
        final ServerBuilder sb = Server.builder();
        sb.disableServerHeader();

        if (httpServerConfig.isSsl()) {
            LOG.info("Creating http source with SSL/TLS enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            // TODO: enable encrypted key with password
            sb.https(httpServerConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
            );
        } else {
            LOG.warn("Creating http source without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the http source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#ssl");
            sb.http(httpServerConfig.getPort());
        }

        if(httpServerConfig.getAuthentication() != null) {
            final Optional<Function<? super HttpService, ? extends HttpService>> optionalAuthDecorator = authenticationProvider.getAuthenticationDecorator();

            if (httpServerConfig.isUnauthenticatedHealthCheck()) {
                optionalAuthDecorator.ifPresent(authDecorator -> sb.decorator(REGEX_HEALTH, authDecorator));
            } else {
                optionalAuthDecorator.ifPresent(sb::decorator);
            }
        }

        sb.maxNumConnections(httpServerConfig.getMaxConnectionCount());
        sb.requestTimeout(Duration.ofMillis(httpServerConfig.getRequestTimeoutInMillis()));
        if(httpServerConfig.getMaxRequestLength() != null) {
            sb.maxRequestLength(httpServerConfig.getMaxRequestLength().getBytes());
        }
        final int threads = httpServerConfig.getThreadCount();
        final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threads);
        sb.blockingTaskExecutor(blockingTaskExecutor, true);
        final int maxPendingRequests = httpServerConfig.getMaxPendingRequests();
        final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(
                maxPendingRequests, blockingTaskExecutor.getQueue());
        final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests, pluginMetrics);

        final String httpSourcePath = httpServerConfig.getPath().replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
        sb.decorator(httpSourcePath, ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));

        if (CompressionOption.NONE.equals(httpServerConfig.getCompression())) {
            sb.annotatedService(httpSourcePath, logService, httpRequestExceptionHandler);
        } else {
            sb.annotatedService(httpSourcePath, logService, DecodingService.newDecorator(), httpRequestExceptionHandler);
        }

        if (httpServerConfig.hasHealthCheckService()) {
            LOG.info("HTTP source health check is enabled");
            sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }

        return sb.build();
    }

    private GrpcExceptionHandlerFunction createGrpExceptionHandler() {
        RetryInfoConfig retryInfo = serverConfiguration.getRetryInfo() != null
                ? serverConfiguration.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
    }

    private List<ServerInterceptor> getAuthenticationInterceptor(final GrpcAuthenticationProvider authenticationProvider) {
        final ServerInterceptor authenticationInterceptor = authenticationProvider.getAuthenticationInterceptor();
        if (authenticationInterceptor == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(authenticationInterceptor);
    }
}
//