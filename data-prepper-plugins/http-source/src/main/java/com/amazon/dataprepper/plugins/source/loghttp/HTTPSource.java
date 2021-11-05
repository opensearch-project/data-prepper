/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.armeria.authentication.ArmeriaAuthenticationProvider;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.source.Source;
import com.amazon.dataprepper.plugins.certificate.CertificateProvider;
import com.amazon.dataprepper.plugins.certificate.model.Certificate;
import com.amazon.dataprepper.plugins.source.loghttp.certificate.CertificateProviderFactory;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.throttling.ThrottlingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@DataPrepperPlugin(name = "http", pluginType = Source.class, pluginConfigurationType = HTTPSourceConfig.class)
public class HTTPSource implements Source<Record<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(HTTPSource.class);

    private final HTTPSourceConfig sourceConfig;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ArmeriaAuthenticationProvider authenticationProvider;
    private Server server;
    private final PluginMetrics pluginMetrics;

    @DataPrepperPluginConstructor
    public HTTPSource(final HTTPSourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        // TODO: Remove once JSR-303 validation is available.
        sourceConfig.validate();
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        certificateProviderFactory = new CertificateProviderFactory(sourceConfig);
        final PluginModel authenticationConfiguration = sourceConfig.getAuthentication();
        final PluginSetting authenticationPluginSetting;
        if(authenticationConfiguration != null) {
            authenticationPluginSetting =
                    new PluginSetting(authenticationConfiguration.getPluginName(), authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting =
                    new PluginSetting(ArmeriaAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        }
        authenticationProvider = pluginFactory.loadPlugin(ArmeriaAuthenticationProvider.class, authenticationPluginSetting);
    }

    @Override
    public void start(final Buffer<Record<String>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            final ServerBuilder sb = Server.builder();
            if (sourceConfig.isSsl()) {
                LOG.info("SSL/TLS is enabled.");
                final CertificateProvider certificateProvider = certificateProviderFactory.getCertificateProvider();
                final Certificate certificate = certificateProvider.getCertificate();
                // TODO: enable encrypted key with password
                sb.https(sourceConfig.getPort()).tls(
                        new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                        new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8)
                        )
                );
            } else {
                sb.http(sourceConfig.getPort());
            }

            authenticationProvider.addAuthenticationDecorator(sb);

            sb.maxNumConnections(sourceConfig.getMaxConnectionCount());
            final int requestTimeoutInMillis = sourceConfig.getRequestTimeoutInMillis();
            // Allow 2*requestTimeoutInMillis to accommodate non-blocking operations other than buffer writing.
            sb.requestTimeout(Duration.ofMillis(2*requestTimeoutInMillis));
            final int threads = sourceConfig.getThreadCount();
            final ScheduledThreadPoolExecutor blockingTaskExecutor = new ScheduledThreadPoolExecutor(threads);
            sb.blockingTaskExecutor(blockingTaskExecutor, true);
            final int maxPendingRequests = sourceConfig.getMaxPendingRequests();
            final LogThrottlingStrategy logThrottlingStrategy = new LogThrottlingStrategy(
                    maxPendingRequests, blockingTaskExecutor.getQueue());
            final LogThrottlingRejectHandler logThrottlingRejectHandler = new LogThrottlingRejectHandler(maxPendingRequests, pluginMetrics);
            // TODO: allow customization on URI path for log ingestion
            sb.decorator(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI, ThrottlingService.newDecorator(logThrottlingStrategy, logThrottlingRejectHandler));
            final LogHTTPService logHTTPService = new LogHTTPService(requestTimeoutInMillis, buffer, pluginMetrics);
            sb.annotatedService(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI, logHTTPService);
            // TODO: attach HealthCheckService

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
        LOG.info("Started http source...");
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
        LOG.info("Stopped http source.");
    }
}
