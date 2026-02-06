package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

import java.time.Duration;
import java.util.Map;

import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;

public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final PluginMetrics pluginMetrics;
    private final OTelTraceSourceConfig oTelTraceSourceConfig;
    private final PluginFactory pluginFactory;

    public HttpService(PluginMetrics pluginMetrics, OTelTraceSourceConfig oTelTraceSourceConfig, PluginFactory pluginFactory) {
        this.pluginMetrics = pluginMetrics;
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
        this.pluginFactory = pluginFactory;
    }

    public ArmeriaHttpService create(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        RetryInfoConfig retryInfo = oTelTraceSourceConfig.getRetryInfo() != null
                ? oTelTraceSourceConfig.getRetryInfo()
                : DEFAULT_RETRY_INFO;
        ArmeriaHttpService httpService = new ArmeriaHttpService(buffer, oTelTraceSourceConfig.getOutputFormat(), pluginMetrics, oTelTraceSourceConfig.getRequestTimeoutInMillis());
        HttpExceptionHandler httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, retryInfo.getMinDelay(), retryInfo.getMaxDelay());

        configureAuthentication(serverBuilder);

        if (CompressionOption.NONE.equals(oTelTraceSourceConfig.getCompression())) {
            serverBuilder.annotatedService(httpService, httpExceptionHandler);
        } else {
            serverBuilder.annotatedService(httpService, DecodingService.newDecorator(), httpExceptionHandler);
        }

        return httpService;
    }

    private void configureAuthentication(ServerBuilder serverBuilder) {
        if (oTelTraceSourceConfig.getAuthentication() == null || oTelTraceSourceConfig.getAuthentication().getPluginName().equals(UNAUTHENTICATED_PLUGIN_NAME)) {
            LOG.warn("Creating otel_trace_source http service without authentication. This is not secure.");
            LOG.warn("In order to set up Http Basic authentication for the otel-trace-source, go here: https://github.com/opensearch-project/data-prepper/tree/main/data-prepper-plugins/otel-trace-source#authentication-configurations");
        } else {
            ArmeriaHttpAuthenticationProvider authenticationProvider = createAuthenticationProvider(oTelTraceSourceConfig.getAuthentication());
            authenticationProvider.getAuthenticationDecorator().ifPresent(serverBuilder::decorator);
        }
    }

    private ArmeriaHttpAuthenticationProvider createAuthenticationProvider(final PluginModel authenticationConfiguration) {
        Map<String, Object> pluginSettings = authenticationConfiguration.getPluginSettings();
        return pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, new PluginSetting(authenticationConfiguration.getPluginName(), pluginSettings));
    }
}
