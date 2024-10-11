package org.opensearch.dataprepper.http;

import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.codec.JsonDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

/**
 * BaseHttpSource class holds the common http related source functionality including starting the armeria server and authentication handling.
 * HTTP based sources should use this functionality when implementing the respective source.
 */
public abstract class BaseHttpSource<T extends Record<?>> implements Source<T> {
    public static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    public static final String SERVER_CONNECTIONS = "serverConnections";
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private final HttpServerConfig sourceConfig;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ArmeriaHttpAuthenticationProvider authenticationProvider;
    private final HttpRequestExceptionHandler httpRequestExceptionHandler;
    private final String pipelineName;
    private final String sourceName;
    private final Logger logger;
    private final PluginMetrics pluginMetrics;
    private Server server;
    private ByteDecoder byteDecoder;

    public BaseHttpSource(final HttpServerConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                          final PipelineDescription pipelineDescription, final String sourceName, final Logger logger) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.sourceName = sourceName;
        this.logger = logger;
        this.byteDecoder = new JsonDecoder();
        this.certificateProviderFactory = new CertificateProviderFactory(sourceConfig);
        final PluginModel authenticationConfiguration = sourceConfig.getAuthentication();
        final PluginSetting authenticationPluginSetting;

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            logger.warn("Creating {} source without authentication. This is not secure.", sourceName);
            logger.warn("In order to set up Http Basic authentication for the {} source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#authentication-configurations", sourceName);
        }

        if (authenticationConfiguration != null) {
            authenticationPluginSetting =
                    new PluginSetting(authenticationConfiguration.getPluginName(), authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting =
                    new PluginSetting(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        authenticationProvider = pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, authenticationPluginSetting);
        httpRequestExceptionHandler = new HttpRequestExceptionHandler(pluginMetrics);
    }

    @Override
    public void start(final Buffer<T> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            final ServerBuilder sb = Server.builder();

            sb.disableServerHeader();

            if (sourceConfig.isSsl()) {
                logger.info("Creating {} source with SSL/TLS enabled.", sourceName);
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                // TODO: enable encrypted key with password
                sb.https(sourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                        )
                );
            } else {
                logger.warn("Creating {} source without SSL/TLS. This is not secure.", sourceName);
                logger.warn("In order to set up TLS for the {} source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#ssl", sourceName);
                sb.http(sourceConfig.getPort());
            }

            if (sourceConfig.getAuthentication() != null) {
                final Optional<Function<? super HttpService, ? extends HttpService>> optionalAuthDecorator = authenticationProvider.getAuthenticationDecorator();

                if (sourceConfig.isUnauthenticatedHealthCheck()) {
                    optionalAuthDecorator.ifPresent(authDecorator -> sb.decorator(REGEX_HEALTH, authDecorator));
                } else {
                    optionalAuthDecorator.ifPresent(sb::decorator);
                }
            }

            sb.maxNumConnections(sourceConfig.getMaxConnectionCount());
            sb.requestTimeout(Duration.ofMillis(sourceConfig.getRequestTimeoutInMillis()));
            if (sourceConfig.getMaxRequestLength() != null) {
                sb.maxRequestLength(sourceConfig.getMaxRequestLength().getBytes());
            }
            final int threads = sourceConfig.getThreadCount();
            final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threads);
            sb.blockingTaskExecutor(blockingTaskExecutor, true);
            final int maxPendingRequests = sourceConfig.getMaxPendingRequests();
            final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(
                    maxPendingRequests, blockingTaskExecutor.getQueue());
            final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests, pluginMetrics);

            final String httpSourcePath = sourceConfig.getPath().replace(PIPELINE_NAME_PLACEHOLDER, pipelineName);
            sb.decorator(httpSourcePath, ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));
            final BaseHttpService httpService = getHttpService(sourceConfig.getBufferTimeoutInMillis(), buffer, pluginMetrics);

            if (CompressionOption.NONE.equals(sourceConfig.getCompression())) {
                sb.annotatedService(httpSourcePath, httpService, httpRequestExceptionHandler);
            } else {
                sb.annotatedService(httpSourcePath, httpService, DecodingService.newDecorator(), httpRequestExceptionHandler);
            }

            if (sourceConfig.hasHealthCheckService()) {
                logger.info("{} source health check is enabled", sourceName);
                sb.service(getHttpHealthCheckPath(), HealthCheckService.builder().longPolling(0).build());
            }

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
        logger.info("Started {} source on port {}", sourceName, sourceConfig.getPort());
    }

    @Override
    public ByteDecoder getDecoder() {
        return byteDecoder;
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
        logger.info("Stopped {} source.", sourceName);
    }

    public abstract BaseHttpService getHttpService(int bufferTimeoutInMillis, Buffer<T> buffer, PluginMetrics pluginMetrics);

    public String getHttpHealthCheckPath() {
        return HTTP_HEALTH_CHECK_PATH;
    }
}
