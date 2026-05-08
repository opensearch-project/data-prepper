/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace.grpc;

import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.server.HealthGrpcService;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

public class GrpcServiceConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcServiceConfigurator.class);
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    private static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    private static final RetryInfoConfig DEFAULT_RETRY_INFO =
            new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final OTelTraceSourceConfig config;
    private final PluginMetrics pluginMetrics;
    private final String pipelineName;
    private final GrpcAuthenticationProvider authenticationProvider;

    public GrpcServiceConfigurator(final OTelTraceSourceConfig config,
                                   final PluginMetrics pluginMetrics,
                                   final String pipelineName,
                                   final GrpcAuthenticationProvider authenticationProvider) {
        this.config = config;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineName;
        this.authenticationProvider = authenticationProvider;
    }

    public void configure(final ServerBuilder sb, final OTelTraceGrpcService grpcService) {
        final GrpcServiceBuilder grpcSB = com.linecorp.armeria.server.grpc.GrpcService.builder()
                .useClientTimeoutHeader(false)
                .useBlockingTaskExecutor(true)
                .exceptionHandler(createGrpcExceptionHandler());

        final List<ServerInterceptor> interceptors = getAuthenticationInterceptors();

        if (config.getPath() != null) {
            final String path = config.getPath().replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
            LOG.info("Configuring gRPC service at custom path: {}", path);
            grpcSB.addService(path,
                    ServerInterceptors.intercept(grpcService, interceptors),
                    grpcService.getExportMethodDescriptor());
        } else {
            grpcSB.addService(ServerInterceptors.intercept(grpcService, interceptors));
        }

        if (config.hasHealthCheck()) {
            LOG.info("gRPC health check service is enabled.");
            grpcSB.addService(new HealthGrpcService());
        }

        if (config.hasProtoReflectionService()) {
            LOG.info("gRPC proto reflection service is enabled.");
            grpcSB.addService(ProtoReflectionService.newInstance());
        }

        grpcSB.enableUnframedRequests(config.enableUnframedRequests());

        final com.linecorp.armeria.server.grpc.GrpcService builtGrpcService = grpcSB.build();
        if (CompressionOption.NONE.equals(config.getCompression())) {
            sb.service(builtGrpcService);
        } else {
            sb.service(builtGrpcService, DecodingService.newDecorator());
        }

        if (config.getAuthentication() != null) {
            authenticationProvider.getHttpAuthenticationService().ifPresent(decorator -> {
                if (config.isUnauthenticatedHealthCheck()) {
                    sb.decorator(REGEX_HEALTH, decorator);
                } else {
                    sb.decorator(decorator);
                }
            });
        }
    }

    private List<ServerInterceptor> getAuthenticationInterceptors() {
        final ServerInterceptor interceptor = authenticationProvider.getAuthenticationInterceptor();
        return interceptor == null ? Collections.emptyList() : Collections.singletonList(interceptor);
    }

    private GrpcExceptionHandlerFunction createGrpcExceptionHandler() {
        final RetryInfoConfig retryInfo = config.getRetryInfo() != null
                ? config.getRetryInfo()
                : DEFAULT_RETRY_INFO;
        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
    }
}
