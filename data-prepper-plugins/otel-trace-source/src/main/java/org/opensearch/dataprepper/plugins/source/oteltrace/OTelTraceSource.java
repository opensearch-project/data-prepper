/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
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
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.function.Function;

@DataPrepperPlugin(name = "otel_trace_source", pluginType = Source.class, pluginConfigurationType = OTelTraceSourceConfig.class)
public class OTelTraceSource implements Source<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    public static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private Server server;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;

    @DataPrepperPluginConstructor
    public OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                           final PipelineDescription pipelineDescription) {
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = new CertificateProviderFactory(oTelTraceSourceConfig);
        this.authenticationProvider = createAuthenticationProvider(pluginFactory, pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                    final CertificateProviderFactory certificateProviderFactory, final PipelineDescription pipelineDescription) {
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.authenticationProvider = createAuthenticationProvider(pluginFactory, pipelineDescription);
    }

    @Override
    public void start(Buffer<Record<Object>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {

            final OTelTraceGrpcService oTelTraceGrpcService = new OTelTraceGrpcService(
                    oTelTraceSourceConfig.getRequestTimeoutInMillis(),
                    new OTelProtoCodec.OTelProtoDecoder(),
                    buffer,
                    pluginMetrics
            );

            final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor();

            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .addService(ServerInterceptors.intercept(oTelTraceGrpcService, serverInterceptors))
                    .useClientTimeoutHeader(false)
                    .useBlockingTaskExecutor(true);

            if (oTelTraceSourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelTraceSourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            grpcServiceBuilder.enableUnframedRequests(oTelTraceSourceConfig.enableUnframedRequests());

            final ServerBuilder sb = Server.builder();
            sb.disableServerHeader();
            sb.service(grpcServiceBuilder.build());

            if(oTelTraceSourceConfig.enableHttpHealthCheck()) {
                sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
            }

            if(oTelTraceSourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService =
                        authenticationProvider.getHttpAuthenticationService();

                if(oTelTraceSourceConfig.isUnauthenticatedHealthCheck()) {
                    optionalHttpAuthenticationService.ifPresent(httpAuthenticationService ->
                            sb.decorator(REGEX_HEALTH, httpAuthenticationService));
                } else {
                    optionalHttpAuthenticationService.ifPresent(sb::decorator);
                }
            }

            sb.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());

            // ACM Cert for SSL takes preference
            if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(oTelTraceSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
                );
            } else {
                LOG.warn("Creating otel_trace_source without SSL/TLS. This is not secure.");
                LOG.warn("In order to set up TLS for the otel_trace_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl");
                sb.http(oTelTraceSourceConfig.getPort());
            }

            sb.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
            sb.blockingTaskExecutor(
                    Executors.newScheduledThreadPool(oTelTraceSourceConfig.getThreadCount()),
                    true);

            server = sb.build();
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
        LOG.info("Started otel_trace_source on port " + oTelTraceSourceConfig.getPort() + "...");
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

    private List<ServerInterceptor> getAuthenticationInterceptor() {
        final ServerInterceptor authenticationInterceptor = authenticationProvider.getAuthenticationInterceptor();
        if (authenticationInterceptor == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(authenticationInterceptor);
    }

    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory, final PipelineDescription pipelineDescription) {
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
        authenticationPluginSetting.setPipelineName(pipelineDescription.getPipelineName());
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, authenticationPluginSetting);
    }
}
