/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;

import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.plugins.otel.codec.OTelTraceDecoder;
import org.opensearch.dataprepper.plugins.source.oteltrace.grpc.GrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.http.ArmeriaHttpService;
import org.opensearch.dataprepper.plugins.source.oteltrace.http.HttpExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

@DataPrepperPlugin(name = "otel_trace_source", pluginType = Source.class, pluginConfigurationType = OTelTraceSourceConfig.class)
public class OTelTraceSource implements Source<Record<Object>> {
    private static final String PLUGIN_NAME = "otel_trace_source";
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceSource.class);

    // todo tlongo include in config
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final CertificateProviderFactory certificateProviderFactory;
    private final String pipelineName;
    private Server server;
    private final ByteDecoder byteDecoder;

    @DataPrepperPluginConstructor
    public OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                           final PipelineDescription pipelineDescription) {
        this(oTelTraceSourceConfig, pluginMetrics, pluginFactory, new CertificateProviderFactory(oTelTraceSourceConfig), pipelineDescription);
    }

    // accessible only in the same package for unit test
    OTelTraceSource(final OTelTraceSourceConfig oTelTraceSourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                    final CertificateProviderFactory certificateProviderFactory, final PipelineDescription pipelineDescription) {
        oTelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
        this.certificateProviderFactory = certificateProviderFactory;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.byteDecoder = new OTelTraceDecoder(oTelTraceSourceConfig.getOutputFormat());
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
            ServerBuilder serverBuilder = Server.builder().port(oTelTraceSourceConfig.getPort(), inferProtocolFromConfig());

            configureHeadersAndHealthCheck(serverBuilder);
            configureTLS(serverBuilder);
            configureTaskExecutor(serverBuilder);

            configureGrpcService(serverBuilder, buffer);
            configureHttpService(serverBuilder, buffer);

            server = serverBuilder.build();

            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
        }
        try {
            server.start().get();
        } catch (ExecutionException ex) {
            handleExecutionException(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
        LOG.info("Started otel_trace_source on port " + oTelTraceSourceConfig.getPort() + "...");
    }

    private SessionProtocol inferProtocolFromConfig() {
        if (oTelTraceSourceConfig.isSsl()) {
            return SessionProtocol.HTTPS;
        } else {
            return SessionProtocol.HTTP;
        }
    }

    private void handleExecutionException(ExecutionException ex) {
        if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
            throw (RuntimeException) ex.getCause();
        } else {
            throw new RuntimeException(ex);
        }
    }

    private void configureGrpcService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        com.linecorp.armeria.server.grpc.GrpcService grpcService = new GrpcService(pluginFactory, oTelTraceSourceConfig, pluginMetrics, pipelineName, certificateProviderFactory).create(buffer, serverBuilder);

        if (CompressionOption.NONE.equals(oTelTraceSourceConfig.getCompression())) {
            serverBuilder.service(grpcService);
        } else {
            serverBuilder.service(grpcService, DecodingService.newDecorator());
        }
    }

    private void configureHttpService(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        ArmeriaHttpService httpService = new ArmeriaHttpService(buffer, pluginMetrics, oTelTraceSourceConfig.getRequestTimeoutInMillis());
        RetryInfoConfig retryInfo = oTelTraceSourceConfig.getRetryInfo() != null
                ? oTelTraceSourceConfig.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        // todo tlongo move creation of handler to ArmeriaHttpService
        HttpExceptionHandler httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());

        configureAuthentication(serverBuilder);

        if (CompressionOption.NONE.equals(oTelTraceSourceConfig.getCompression())) {
            serverBuilder.annotatedService(httpService, httpExceptionHandler);
        } else {
            serverBuilder.annotatedService(httpService, DecodingService.newDecorator(), httpExceptionHandler);
        }
    }

    // todo tlongo move to http service -> Create additional layer. See GrpcService
    private void configureAuthentication(ServerBuilder serverBuilder) {
        if (oTelTraceSourceConfig.getAuthentication() == null || oTelTraceSourceConfig.getAuthentication().getPluginName().equals(UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel_trace_source http service without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-trace-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations");
        } else {
            ArmeriaHttpAuthenticationProvider authenticationProvider = createAuthenticationProvider(oTelTraceSourceConfig.getAuthentication());
            authenticationProvider.getAuthenticationDecorator().ifPresent(serverBuilder::decorator);
        }
    }

    // todo tlongo move to http service -> Create additional layer. See GrpcService
    private ArmeriaHttpAuthenticationProvider createAuthenticationProvider(final PluginModel authenticationConfiguration) {
        Map<String, Object> pluginSettings = authenticationConfiguration.getPluginSettings();

        // controversial
        // the world would be a nicer place, if mere configs were not be treated as plugins
        // this method replaces the process of
        //       yaml -> pluginmodel -> pluginsettings -> configPojo -> pluginfactory -> provider
        // with
        //       yaml -> configPojo -> provider (we could eliminate using Plugin* Classes all together by parsing the yaml section at startup, e.g. like retryInfo)
        // pros:
        //   - we can easily reason about the origins of the provider
        //   - it becomes testable
        // cons:
        //   - currently tied to one impl by using 'new'.
        return new HttpBasicArmeriaHttpAuthenticationProvider(new HttpBasicAuthenticationConfig(pluginSettings.get("username").toString(), pluginSettings.get("password").toString()));
    }

    private void configureHeadersAndHealthCheck(ServerBuilder serverBuilder) {
        serverBuilder.disableServerHeader();
        if (oTelTraceSourceConfig.enableHttpHealthCheck()) {
            serverBuilder.service(HTTP_HEALTH_CHECK_PATH, HealthCheckService.builder().longPolling(0).build());
        }
        serverBuilder.requestTimeoutMillis(oTelTraceSourceConfig.getRequestTimeoutInMillis());
        if(oTelTraceSourceConfig.getMaxRequestLength() != null) {
            serverBuilder.maxRequestLength(oTelTraceSourceConfig.getMaxRequestLength().getBytes());
        }
        serverBuilder.maxNumConnections(oTelTraceSourceConfig.getMaxConnectionCount());
    }

    private void configureTLS(ServerBuilder serverBuilder) {
        if (oTelTraceSourceConfig.isSsl() || oTelTraceSourceConfig.useAcmCertForSSL()) {
            LOG.info("SSL/TLS is enabled.");
            final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
            final Certificate certificate = certificateProvider.getCertificate();
            serverBuilder.https(oTelTraceSourceConfig.getPort()).tls(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                    )
            );
        } else {
            LOG.warn("Creating otel_trace_source without SSL/TLS. This is not secure.");
            LOG.warn("In order to set up TLS for the otel_trace_source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#ssl");
            serverBuilder.http(oTelTraceSourceConfig.getPort());
        }
    }

    private void configureTaskExecutor(ServerBuilder serverBuilder) {
        final BlockingTaskExecutor blockingTaskExecutor = BlockingTaskExecutor.builder()
                .numThreads(oTelTraceSourceConfig.getThreadCount())
                .threadNamePrefix(pipelineName + "-otel_trace")
                .build();
        serverBuilder.blockingTaskExecutor(blockingTaskExecutor, true);
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
}
