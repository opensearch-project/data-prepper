/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

public class HttpServiceConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(HttpServiceConfigurator.class);
    private static final RetryInfoConfig DEFAULT_RETRY_INFO =
            new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final OTelTraceSourceConfig config;
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;

    public HttpServiceConfigurator(final OTelTraceSourceConfig config,
                                   final PluginMetrics pluginMetrics,
                                   final PluginFactory pluginFactory) {
        this.config = config;
        this.pluginMetrics = pluginMetrics;
        this.pluginFactory = pluginFactory;
    }

    public void configure(final ServerBuilder sb,
                          final OTelProtoCodec.OTelProtoDecoder decoder,
                          final Buffer<Record<Object>> buffer) {
        LOG.info("Configuring HTTP service.");

        final RetryInfoConfig retryInfo = config.getRetryInfo() != null
                ? config.getRetryInfo()
                : DEFAULT_RETRY_INFO;

        final ArmeriaHttpService httpService = new ArmeriaHttpService(
                buffer, decoder, pluginMetrics, (int) (config.getRequestTimeoutInMillis() * 0.8));

        final HttpExceptionHandler exceptionHandler =
                new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());

        configureAuthentication(sb);

        if (CompressionOption.NONE.equals(config.getCompression())) {
            sb.annotatedService(httpService, exceptionHandler);
        } else {
            sb.annotatedService(httpService, DecodingService.newDecorator(), exceptionHandler);
        }
    }

    private void configureAuthentication(final ServerBuilder sb) {
        final PluginModel authConfig = config.getAuthentication();
        if (authConfig == null || authConfig.getPluginName().equals(UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating {} HTTP service without authentication. This is not secure.", "otlp_traces");
            LOG.warn("In order to set up Http Basic authentication for the {}, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations", "otlp_traces");
        } else {
            final ArmeriaHttpAuthenticationProvider authProvider = pluginFactory.loadPlugin(
                    ArmeriaHttpAuthenticationProvider.class,
                    new PluginSetting(authConfig.getPluginName(), authConfig.getPluginSettings()));
            authProvider.getAuthenticationDecorator().ifPresent(sb::decorator);
        }
    }
}
