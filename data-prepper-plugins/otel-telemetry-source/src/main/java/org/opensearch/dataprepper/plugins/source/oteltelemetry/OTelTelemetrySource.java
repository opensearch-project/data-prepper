/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;

import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsGrpcService;
import org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltelemetry.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

@DataPrepperPlugin(name = "otel_telemetry_source", pluginType = Source.class, pluginConfigurationType = OTelTelemetrySourceConfig.class)
public class OTelTelemetrySource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTelemetrySource.class);
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    static final String SERVER_CONNECTIONS = "serverConnections";

    // Default RetryInfo with minimum 100ms and maximum 2s
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100),
            Duration.ofMillis(2000));
    private final OTelTelemetrySourceConfig oTelTelemetrySourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private Server server;

    @DataPrepperPluginConstructor
    public OTelTelemetrySource(final OTelTelemetrySourceConfig oTelTelemetrySourceConfig,
            final PluginMetrics pluginMetrics,
            final PluginFactory pluginFactory,
            final PipelineDescription pipelineDescription) {
        this(oTelTelemetrySourceConfig, pluginMetrics, pluginFactory,
                new CertificateProviderFactory(oTelTelemetrySourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelTelemetrySource(final OTelTelemetrySourceConfig oTelTelemetrySourceConfig,
            final PluginMetrics pluginMetrics,
            final PluginFactory pluginFactory,
            final CertificateProviderFactory certificateProviderFactory,
            final PipelineDescription pipelineDescription) {
        oTelTelemetrySourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTelemetrySourceConfig = oTelTelemetrySourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {
            @SuppressWarnings("unchecked")
            Buffer<Record<? extends Metric>> metricBuffer = (Buffer<Record<? extends Metric>>) (Object) buffer;

            final OTelLogsGrpcService oTelLogsGrpcService = new OTelLogsGrpcService(
                    (int) (oTelTelemetrySourceConfig.getRequestTimeoutInMillis() * 0.8),
                    new OTelProtoStandardCodec.OTelProtoDecoder(),
                    buffer, pluginMetrics);

            final OTelMetricsGrpcService oTelMetricsGrpcService = new OTelMetricsGrpcService(
                    (int) (oTelTelemetrySourceConfig.getRequestTimeoutInMillis() * 0.8),
                    new OTelProtoStandardCodec.OTelProtoDecoder(),
                    metricBuffer, pluginMetrics);

            final OTelTraceGrpcService oTelTraceGrpcService = new OTelTraceGrpcService(
                    (int) (oTelTelemetrySourceConfig.getRequestTimeoutInMillis() * 0.8),
                    new OTelProtoStandardCodec.OTelProtoDecoder(),
                    buffer, pluginMetrics);

            final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor();

            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .useClientTimeoutHeader(false)
                    .useBlockingTaskExecutor(true)
                    .exceptionHandler(createGrpExceptionHandler());

            final MethodDescriptor<ExportLogsServiceRequest, ExportLogsServiceResponse> methodLogsDescriptor = LogsServiceGrpc
                    .getExportMethod();
            final MethodDescriptor<ExportMetricsServiceRequest, ExportMetricsServiceResponse> methodMetricsDescriptor = MetricsServiceGrpc
                    .getExportMethod();
            final MethodDescriptor<ExportTraceServiceRequest, ExportTraceServiceResponse> methodTraceDescriptor = TraceServiceGrpc
                    .getExportMethod();

            final String oTelLogsSourcePath = oTelTelemetrySourceConfig.getLogsPath();
            final String oTelMetricsSourcePath = oTelTelemetrySourceConfig.getMetricsPath();
            final String oTelTraceSourcePath = oTelTelemetrySourceConfig.getTracesPath();

            // If the path is null for any of the sources, we will not transform the path
            // and use the default path
            if (oTelTraceSourcePath != null && oTelMetricsSourcePath != null && oTelLogsSourcePath != null) {
                final String transformedOTelLogsSourcePath = oTelLogsSourcePath.replace(PIPELINE_NAME_PLACEHOLDER,
                        pipelineName);
                final String transformedOTelMetricsSourcePath = oTelMetricsSourcePath.replace(PIPELINE_NAME_PLACEHOLDER,
                        pipelineName);
                final String transformedOTelTraceSourcePath = oTelTraceSourcePath.replace(PIPELINE_NAME_PLACEHOLDER,
                        pipelineName);
                grpcServiceBuilder.addService(transformedOTelLogsSourcePath,
                        ServerInterceptors.intercept(oTelLogsGrpcService, serverInterceptors), methodLogsDescriptor);
                grpcServiceBuilder.addService(transformedOTelMetricsSourcePath,
                        ServerInterceptors.intercept(oTelMetricsGrpcService, serverInterceptors),
                        methodMetricsDescriptor);
                grpcServiceBuilder.addService(transformedOTelTraceSourcePath,
                        ServerInterceptors.intercept(oTelTraceGrpcService, serverInterceptors), methodTraceDescriptor);

            } else {
                grpcServiceBuilder.addService(ServerInterceptors.intercept(oTelLogsGrpcService, serverInterceptors));
                grpcServiceBuilder.addService(ServerInterceptors.intercept(oTelMetricsGrpcService, serverInterceptors));
                grpcServiceBuilder.addService(ServerInterceptors.intercept(oTelTraceGrpcService, serverInterceptors));
            }

            if (oTelTelemetrySourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelTelemetrySourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            grpcServiceBuilder.enableUnframedRequests(oTelTelemetrySourceConfig.enableUnframedRequests());

            final ServerBuilder sb = Server.builder();
            sb.disableServerHeader();
            if (CompressionOption.NONE.equals(oTelTelemetrySourceConfig.getCompression())) {
                sb.service(grpcServiceBuilder.build());
            } else {
                sb.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
            }

            if (oTelTelemetrySourceConfig.enableHttpHealthCheck()) {
                sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
            }

            if (oTelTelemetrySourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService = authenticationProvider
                        .getHttpAuthenticationService();

                if (oTelTelemetrySourceConfig.isUnauthenticatedHealthCheck()) {
                    optionalHttpAuthenticationService.ifPresent(
                            httpAuthenticationService -> sb.decorator(REGEX_HEALTH, httpAuthenticationService));
                } else {
                    optionalHttpAuthenticationService.ifPresent(sb::decorator);
                }
            }

            sb.requestTimeoutMillis(oTelTelemetrySourceConfig.getRequestTimeoutInMillis());
            if (oTelTelemetrySourceConfig.getMaxRequestLength() != null) {
                sb.maxRequestLength(oTelTelemetrySourceConfig.getMaxRequestLength().getBytes());
            }

            // ACM Cert for SSL takes preference
            if (oTelTelemetrySourceConfig.isSsl() || oTelTelemetrySourceConfig.useAcmCertForSSL()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(oTelTelemetrySourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)));
            } else {
                LOG.warn("Creating otel_trace_source without SSL/TLS. This is not secure.");
                LOG.warn(
                        "In order to set up TLS for the otel_telemetry_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-telemetry-source#ssl");
                sb.http(oTelTelemetrySourceConfig.getPort());
            }

            sb.maxNumConnections(oTelTelemetrySourceConfig.getMaxConnectionCount());
            final BlockingTaskExecutor blockingTaskExecutor = BlockingTaskExecutor.builder()
                    .numThreads(oTelTelemetrySourceConfig.getThreadCount())
                    .threadNamePrefix(pipelineName + "-otel_telemetry")
                    .build();
            sb.blockingTaskExecutor(blockingTaskExecutor, true);

            server = sb.build();

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
        LOG.info("Started otel_telemetry_source on port " + oTelTelemetrySourceConfig.getPort() + "...");
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
        LOG.info("Stopped otel_telemetry_source.");
    }

    private GrpcExceptionHandlerFunction createGrpExceptionHandler() {
        RetryInfoConfig retryInfo = oTelTelemetrySourceConfig.getRetryInfo() != null
                ? oTelTelemetrySourceConfig.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
    }

    private List<ServerInterceptor> getAuthenticationInterceptor() {
        final ServerInterceptor authenticationInterceptor = authenticationProvider.getAuthenticationInterceptor();
        if (authenticationInterceptor == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(authenticationInterceptor);
    }

    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory) {
        final PluginModel authenticationConfiguration = oTelTelemetrySourceConfig.getAuthentication();

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName()
                .equals(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel-telemetry-source without authentication. This is not secure.");
            LOG.warn(
                    "In order to set up Http Basic authentication for the otel-logs-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-telemetry-source#authentication-configurations");
        }

        final PluginSetting authenticationPluginSetting;
        if (authenticationConfiguration != null) {
            authenticationPluginSetting = new PluginSetting(authenticationConfiguration.getPluginName(),
                    authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting = new PluginSetting(GrpcAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
                    Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, authenticationPluginSetting);
    }
}
