/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics;

import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import com.amazon.dataprepper.plugins.health.HealthGrpcService;
import com.amazon.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;

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

@DataPrepperPlugin(name = "otel_metrics_source", pluginType = Source.class, pluginConfigurationType = OTelMetricsSourceConfig.class)
public class OTelMetricsSource implements Source<Record<ExportMetricsServiceRequest>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsSource.class);
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    private final OTelMetricsSourceConfig oTelMetricsSourceConfig;
    private Server server;
    private final PluginMetrics pluginMetrics;
    private final GrpcAuthenticationProvider authenticationProvider;
    private final CertificateProviderFactory certificateProviderFactory;

    @DataPrepperPluginConstructor
    public OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        oTelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelMetricsSourceConfig = oTelMetricsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = new CertificateProviderFactory(oTelMetricsSourceConfig);
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
    }

    // accessible only in the same package for unit test
    OTelMetricsSource(final OTelMetricsSourceConfig oTelMetricsSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final CertificateProviderFactory certificateProviderFactory) {
        oTelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelMetricsSourceConfig = oTelMetricsSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.certificateProviderFactory = certificateProviderFactory;
        this.authenticationProvider = createAuthenticationProvider(pluginFactory);
    }

    @Override
    public void start(Buffer<Record<ExportMetricsServiceRequest>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {

            final OTelMetricsGrpcService oTelMetricsGrpcService = new OTelMetricsGrpcService(
                    oTelMetricsSourceConfig.getRequestTimeoutInMillis(),
                    buffer,
                    pluginMetrics
            );

            final List<ServerInterceptor> serverInterceptors = getAuthenticationInterceptor();

            final GrpcServiceBuilder grpcServiceBuilder = GrpcService
                    .builder()
                    .addService(ServerInterceptors.intercept(oTelMetricsGrpcService, serverInterceptors))
                    .useClientTimeoutHeader(false)
                    .useBlockingTaskExecutor(true);

            if (oTelMetricsSourceConfig.hasHealthCheck()) {
                LOG.info("Health check is enabled");
                grpcServiceBuilder.addService(new HealthGrpcService());
            }

            if (oTelMetricsSourceConfig.hasProtoReflectionService()) {
                LOG.info("Proto reflection service is enabled");
                grpcServiceBuilder.addService(ProtoReflectionService.newInstance());
            }

            grpcServiceBuilder.enableUnframedRequests(oTelMetricsSourceConfig.enableUnframedRequests());

            final ServerBuilder sb = Server.builder();
            sb.disableServerHeader();
            sb.service(grpcServiceBuilder.build());

            if(oTelMetricsSourceConfig.enableHttpHealthCheck()) {
                sb.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
            }

            if(oTelMetricsSourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalHttpAuthenticationService =
                        authenticationProvider.getHttpAuthenticationService();

                if(oTelMetricsSourceConfig.isUnauthenticatedHealthCheck()) {
                    optionalHttpAuthenticationService.ifPresent(httpAuthenticationService ->
                            sb.decorator(REGEX_HEALTH, httpAuthenticationService));
                } else {
                    optionalHttpAuthenticationService.ifPresent(sb::decorator);
                }
            }

            sb.requestTimeoutMillis(oTelMetricsSourceConfig.getRequestTimeoutInMillis());

            // ACM Cert for SSL takes preference
            if (oTelMetricsSourceConfig.isSsl() || oTelMetricsSourceConfig.useAcmCertForSSL()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                sb.https(oTelMetricsSourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                        )
                );
            } else {
                LOG.warn("Creating otel_metrics_source without SSL/TLS. This is not secure.");
                LOG.warn("In order to set up TLS for the otel_metrics_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-metrics-source#ssl");
                sb.http(oTelMetricsSourceConfig.getPort());
            }

            sb.maxNumConnections(oTelMetricsSourceConfig.getMaxConnectionCount());
            sb.blockingTaskExecutor(
                    Executors.newScheduledThreadPool(oTelMetricsSourceConfig.getThreadCount()),
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
        LOG.info("Started otel_metrics_source...");
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

    private List<ServerInterceptor> getAuthenticationInterceptor() {
        final ServerInterceptor authenticationInterceptor = authenticationProvider.getAuthenticationInterceptor();
        if (authenticationInterceptor == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(authenticationInterceptor);
    }

    private GrpcAuthenticationProvider createAuthenticationProvider(final PluginFactory pluginFactory) {
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
        return pluginFactory.loadPlugin(GrpcAuthenticationProvider.class, authenticationPluginSetting);
    }
}
