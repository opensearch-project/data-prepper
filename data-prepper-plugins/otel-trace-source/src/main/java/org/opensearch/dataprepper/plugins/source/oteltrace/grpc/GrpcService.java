package org.opensearch.dataprepper.plugins.source.oteltrace.grpc;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;

import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;

public class GrpcService {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcService.class);

    // Default RetryInfo with minimum 100ms and maximum 2s
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    public static final String REGEX_HEALTH = "regex:^/(?!health$).*$";

    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final PluginMetrics pluginMetrics;
    private final String pipelineName;
    private final CertificateProviderFactory certificateProviderFactory;

    public GrpcService(PluginFactory pluginFactory, OTelTraceSourceConfig oTelTraceSourceConfig, PluginMetrics pluginMetrics, String pipelineName, CertificateProviderFactory certificateProviderFactory) {
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineName;
        this.authenticationProvider = createAuthenticationProvider(pluginFactory, oTelTraceSourceConfig);
        this.certificateProviderFactory = certificateProviderFactory;
    }

    public void create(Buffer<Record<Object>> buffer, ServerBuilder serverBuilder) {

        final OTelTraceGrpcService oTelTraceGrpcService = new OTelTraceGrpcService(
                (int)(oTelTraceSourceConfig.getRequestTimeoutInMillis() * 0.8),
                new OTelProtoCodec.OTelProtoDecoder(),
                buffer,
                pluginMetrics
        );

        final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor();

        final GrpcServiceBuilder grpcServiceBuilder = com.linecorp.armeria.server.grpc.GrpcService
                .builder()
                .useClientTimeoutHeader(false)
                .useBlockingTaskExecutor(true)
                .exceptionHandler(createGrpExceptionHandler());

        final MethodDescriptor<ExportTraceServiceRequest, ExportTraceServiceResponse> methodDescriptor = TraceServiceGrpc.getExportMethod();
        final String oTelTraceSourcePath = oTelTraceSourceConfig.getPath();
        if (oTelTraceSourcePath != null) {
            final String transformedOTelTraceSourcePath = oTelTraceSourcePath.replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
            grpcServiceBuilder.addService(transformedOTelTraceSourcePath,
                    ServerInterceptors.intercept(oTelTraceGrpcService, serverInterceptors), methodDescriptor);
        } else {
            grpcServiceBuilder.addService(ServerInterceptors.intercept(oTelTraceGrpcService, serverInterceptors));
        }

        if (oTelTraceSourceConfig.hasHealthCheck()) {
            LOG.info("Health check is enabled");
            grpcServiceBuilder.addService(new HealthGrpcService());
        }

        if (oTelTraceSourceConfig.hasProtoReflectionService()) {
            LOG.info("Proto reflection service is enabled");
            grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
        }

        // todo tlongo let this method return the grpc service. All things serverbuilder related have to be done by the source

        grpcServiceBuilder.enableUnframedRequests(oTelTraceSourceConfig.enableUnframedRequests());

//        serverBuilder.disableServerHeader();
        if (CompressionOption.NONE.equals(oTelTraceSourceConfig.getCompression())) {
            serverBuilder.service(grpcServiceBuilder.build());
        } else {
            serverBuilder.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
        }

//        if (oTelTraceSourceConfig.enableHttpHealthCheck()) {
//            serverBuilder.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
//        }

        if (oTelTraceSourceConfig.getAuthentication() != null) {
            final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService =
                    authenticationProvider.getHttpAuthenticationService();

            if (oTelTraceSourceConfig.isUnauthenticatedHealthCheck()) {
                optionalHttpAuthenticationService.ifPresent(httpAuthenticationService ->
                        serverBuilder.decorator(REGEX_HEALTH, httpAuthenticationService));
            } else {
                optionalHttpAuthenticationService.ifPresent(serverBuilder::decorator);
            }
        }

//        serverBuilder.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());
//        if(oTelTraceSourceConfig.getMaxRequestLength() != null) {
//            serverBuilder.maxRequestLength(oTelTraceSourceConfig.getMaxRequestLength().getBytes());
//        }

        // ACM Cert for SSL takes preference
//        if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) { LOG.info("SSL/TLS is enabled.");
//            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
//            final Certificate certificate = certificateProvider.getCertificate();
//            serverBuilder.https(oTelTraceSourceConfig.getPort()).tls(
//                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
//                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
//                    )
//            );
//        } else {
//            LOG.warn("Creating otel_trace_source without SSL/TLS. This is not secure.");
//            LOG.warn("In order to set up TLS for the otel_trace_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl");
//            serverBuilder.http(oTelTraceSourceConfig.getPort());
//        }

//        serverBuilder.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
//        final BlockingTaskExecutor blockingTaskExecutor = BlockingTaskExecutor.builder()
//                .numThreads(oTelTraceSourceConfig.getThreadCount())
//                .threadNamePrefix(pipelineName + "-otel_trace")
//                .build();
//        serverBuilder.blockingTaskExecutor(blockingTaskExecutor, true);
    }

    private List<ServerInterceptor> getAuthenticationInterceptor() {
        final ServerInterceptor authenticationInterceptor = authenticationProvider.getAuthenticationInterceptor();
        if (authenticationInterceptor == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(authenticationInterceptor);
    }

    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory, final OTelTraceSourceConfig oTelTraceSourceConfig) {
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

    private GrpcExceptionHandlerFunction createGrpExceptionHandler() {
        RetryInfoConfig retryInfo = oTelTraceSourceConfig.getRetryInfo() != null
                ? oTelTraceSourceConfig.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        return new GrpcRequestExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());
    }
}
