package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import static org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME;

import java.time.Duration;
import java.util.Map;

import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.RetryInfoConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.encoding.DecodingService;

public class HttpService {
    private static final Logger LOG = LoggerFactory.getLogger(HttpService.class);
    // todo tlongo include in config
    private static final RetryInfoConfig DEFAULT_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(100), Duration.ofMillis(2000));

    private final PluginMetrics pluginMetrics;
    private final OTelTraceSourceConfig oTelTraceSourceConfig;

    public HttpService(PluginMetrics pluginMetrics, OTelTraceSourceConfig oTelTraceSourceConfig) {
        this.pluginMetrics = pluginMetrics;
        this.oTelTraceSourceConfig = oTelTraceSourceConfig;
    }

    public ArmeriaHttpService create(ServerBuilder serverBuilder, Buffer<Record<Object>> buffer) {
        RetryInfoConfig retryInfo = oTelTraceSourceConfig.getRetryInfo() != null
                ? oTelTraceSourceConfig.getRetryInfo()
                : DEFAULT_RETRY_INFO;
        ArmeriaHttpService httpService = new ArmeriaHttpService(buffer, pluginMetrics, oTelTraceSourceConfig.getRequestTimeoutInMillis());
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
}
