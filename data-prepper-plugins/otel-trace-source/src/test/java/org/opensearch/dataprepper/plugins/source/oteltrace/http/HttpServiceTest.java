package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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

import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import com.linecorp.armeria.server.ServerBuilder;

import java.util.HashMap;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class HttpServiceTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private OTelTraceSourceConfig oTelTraceSourceConfig;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private ArmeriaHttpAuthenticationProvider authenticationProvider;

    private HttpService httpService;

    @BeforeEach
    void setUp() {
        final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder = new OTelProtoOpensearchCodec.OTelProtoDecoder();
        httpService = new HttpService(pluginMetrics, otelProtoDecoder, oTelTraceSourceConfig, pluginFactory);
    }

    @Test
    void testCreateWithNoCompression() {
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(null);

        ArmeriaHttpService result = httpService.create(serverBuilder, buffer);

        assertNotNull(result);
        verify(serverBuilder).annotatedService(any(ArmeriaHttpService.class), any(HttpExceptionHandler.class));
    }

    @Test
    void testCreateWithCompression() {
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(null);

        ArmeriaHttpService result = httpService.create(serverBuilder, buffer);

        assertNotNull(result);
        verify(serverBuilder).annotatedService(any(ArmeriaHttpService.class), any(), any(HttpExceptionHandler.class));
    }

    @Test
    void testCreateWithCustomRetryInfo() {
        RetryInfoConfig retryInfo = new RetryInfoConfig(Duration.ofMillis(200), Duration.ofMillis(3000));
        when(oTelTraceSourceConfig.getRetryInfo()).thenReturn(retryInfo);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(null);

        ArmeriaHttpService result = httpService.create(serverBuilder, buffer);

        assertNotNull(result);
    }

    @Test
    void testCreateWithAuthentication() {
        PluginModel authConfig = mock(PluginModel.class);
        when(authConfig.getPluginName()).thenReturn("http_basic");
        when(authConfig.getPluginSettings()).thenReturn(new HashMap<>());
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(authConfig);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        when(authenticationProvider.getAuthenticationDecorator()).thenReturn(Optional.empty());

        ArmeriaHttpService result = httpService.create(serverBuilder, buffer);

        assertNotNull(result);
        verify(pluginFactory).loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class));
    }
}
