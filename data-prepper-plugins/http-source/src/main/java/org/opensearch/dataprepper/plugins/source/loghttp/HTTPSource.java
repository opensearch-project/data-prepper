/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.http.HttpServerConfig;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.JsonDecoder;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@DataPrepperPlugin(name = "http", pluginType = Source.class, pluginConfigurationType = HTTPSourceConfig.class)
public class HTTPSource implements Source<Record<Log>> {
    private static final String PLUGIN_NAME = "http";
    private static final Logger LOG = LoggerFactory.getLogger(HTTPSource.class);
    private static final String PIPELINE_NAME_PLACEHOLDER = "${pipelineName}";
    public static final String REGEX_HEALTH = "regex:^/(?!health$).*$";
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final HttpServerConfig sourceConfig;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ArmeriaHttpAuthenticationProvider authenticationProvider;
    private final HttpRequestExceptionHandler httpRequestExceptionHandler;
    private final String pipelineName;
    private Server server;
    private final PluginMetrics pluginMetrics;
    private static final String HTTP_HEALTH_CHECK_PATH = "/health";
    private ByteDecoder byteDecoder;
    private final InputCodec codec;

    @DataPrepperPluginConstructor
    public HTTPSource(final HTTPSourceConfig sourceConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory,
                      final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.byteDecoder = new JsonDecoder();
        this.certificateProviderFactory = new CertificateProviderFactory(sourceConfig);
        final PluginModel authenticationConfiguration = sourceConfig.getAuthentication();
        final PluginSetting authenticationPluginSetting;

        if (authenticationConfiguration == null || authenticationConfiguration.getPluginName().equals(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating http source without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the http source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/http-source#authentication-configurations");
        }

        if(authenticationConfiguration != null) {
            authenticationPluginSetting =
                    new PluginSetting(authenticationConfiguration.getPluginName(), authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting =
                    new PluginSetting(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME, Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        authenticationProvider = pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, authenticationPluginSetting);
        httpRequestExceptionHandler = new HttpRequestExceptionHandler(pluginMetrics);
        final PluginModel codecConfiguration = sourceConfig.getCodec();
        if (codecConfiguration == null) {
            codec = null;
        } else {
            final PluginSetting codecPluginSettings = new PluginSetting(codecConfiguration.getPluginName(), codecConfiguration.getPluginSettings());
            codec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSettings);
        }
    }

    @Override
    public void start(final Buffer<Record<Log>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        if (server == null) {
            ServerConfiguration serverConfiguration = ConvertConfiguration.convertConfiguration(sourceConfig);
            CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, PLUGIN_NAME, pipelineName);
            final LogHTTPService logHTTPService = new LogHTTPService(serverConfiguration.getBufferTimeoutInMillis(), buffer, pluginMetrics, codec);
            server = createServer.createHTTPServer(buffer, certificateProviderFactory, authenticationProvider, httpRequestExceptionHandler, logHTTPService);
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
        LOG.info("Started http source on port " + sourceConfig.getPort() + "...");
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
        LOG.info("Stopped http source.");
    }
}
