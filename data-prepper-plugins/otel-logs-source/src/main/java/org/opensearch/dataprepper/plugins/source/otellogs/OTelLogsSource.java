/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.linecorp.armeria.common.grpc.GrpcExceptionHandlerFunction;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.encoding.DecodingService;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Function;

@DataPrepperPlugin(name = "otel_logs_source", pluginType = Source.class, pluginConfigurationType = OTelLogsSourceConfig.class)
public class OTelLogsSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsSource.class);
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    static final String SERVER_CONNECTIONS = "serverConnections";

    // Default RetryInfo with minimum 100ms and maximum 2s
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final OTelLogsSourceConfig oTelLogsSourceConfig;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ByteDecoder byteDecoder;
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
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
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

        if (server == null) {

            final OTelLogsGrpcService oTelLogsGrpcService = new OTelLogsGrpcService(
                    (int) (oTelLogsSourceConfig.getRequestTimeoutInMillis() * 0.8),
                    oTelLogsSourceConfig.getOutputFormat() == OTelOutputFormat.OPENSEARCH ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder(),
                    buffer,
                    pluginMetrics
            );

            final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor();

            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .useClientTimeoutHeader(false)
                    .useBlockingTaskExecutor(true)
                    .exceptionHandler(createGrpExceptionHandler());

            final MethodDescriptor<ExportLogsServiceRequest, ExportLogsServiceResponse> methodDescriptor = LogsServiceGrpc.getExportMethod();
            final String oTelLogsSourcePath = oTelLogsSourceConfig.getPath();
            if (oTelLogsSourcePath != null) {
                final String transformedOTelLogsSourcePath = oTelLogsSourcePath.replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
                grpcServiceBuilder.addService(transformedOTelLogsSourcePath,
                        ServerInterceptors.intercept(oTelLogsGrpcService, serverInterceptors), methodDescriptor);
            } else {
                grpcServiceBuilder.addService(ServerInterceptors.intercept(oTelLogsGrpcService, serverInterceptors));
            }

            if (oTelLogsSourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelLogsSourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            grpcServiceBuilder.enableUnframedRequests(oTelLogsSourceConfig.enableUnframedRequests());

            final ServerBuilder sb = Server.builder();
            sb.disableServerHeader();
            if (CompressionOption.NONE.equals(oTelLogsSourceConfig.getCompression())) {
                sb.service(grpcServiceBuilder.build());
            } else {
                sb.service(grpcServiceBuilder.build(), DecodingService.newDecorator());
            }

            if (oTelLogsSourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService =
                        authenticationProvider.getHttpAuthenticationService();
                optionalHttpAuthenticationService.ifPresent(sb::decorator);
            }

            sb.requestTimeoutMillis(oTelLogsSourceConfig.getRequestTimeoutInMillis());
            if(oTelLogsSourceConfig.getMaxRequestLength() != null) {
                sb.maxRequestLength(oTelLogsSourceConfig.getMaxRequestLength().getBytes());
            }

            // ACM Cert for SSL takes preference
            if (oTelLogsSourceConfig.isSsl() || oTelLogsSourceConfig.useAcmCertForSSL()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(oTelLogsSourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                        )
                );
            } else {
                LOG.warn("Creating otel_logs_source without SSL/TLS. This is not secure.");
                LOG.warn("In order to set up TLS for the otel_logs_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-logs-source#ssl");
                sb.http(oTelLogsSourceConfig.getPort());
            }

            sb.maxNumConnections(oTelLogsSourceConfig.getMaxConnectionCount());
            sb.blockingTaskExecutor(
                    Executors.newScheduledThreadPool(oTelLogsSourceConfig.getThreadCount()),
                    true);

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
        LOG.info("Started otel_logs_source...");
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

    private GrpcExceptionHandlerFunction createGrpExceptionHandler() {
        RetryInfoConfig retryInfo = oTelLogsSourceConfig.getRetryInfo() != null
                ? oTelLogsSourceConfig.getRetryInfo()
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
}
