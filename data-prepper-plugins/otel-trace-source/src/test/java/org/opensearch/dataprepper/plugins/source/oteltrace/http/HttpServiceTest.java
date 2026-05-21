package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig;

import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import com.linecorp.armeria.server.ServerBuilder;

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

    private HttpServiceConfigurator httpServiceConfigurator;

    @BeforeEach
    void setUp() {
        httpServiceConfigurator = new HttpServiceConfigurator(oTelTraceSourceConfig, pluginMetrics, pluginFactory);
    }

    @Test
    void testConfigureWithNoCompression() {
        final OTelProtoCodec.OTelProtoDecoder decoder = new OTelProtoOpensearchCodec.OTelProtoDecoder();
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);

        httpServiceConfigurator.configure(serverBuilder, decoder, buffer);

        verify(serverBuilder).annotatedService(any(ArmeriaHttpService.class), any(HttpExceptionHandler.class));
    }

    @Test
    void testConfigureWithCompression() {
        final OTelProtoCodec.OTelProtoDecoder decoder = new OTelProtoOpensearchCodec.OTelProtoDecoder();
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);

        httpServiceConfigurator.configure(serverBuilder, decoder, buffer);

        verify(serverBuilder).annotatedService(any(ArmeriaHttpService.class), any(), any(HttpExceptionHandler.class));
    }

    @Test
    void testConfigureWithCustomRetryInfo() {
        final OTelProtoCodec.OTelProtoDecoder decoder = new OTelProtoOpensearchCodec.OTelProtoDecoder();
        final RetryInfoConfig retryInfo = new RetryInfoConfig(Duration.ofMillis(200), Duration.ofMillis(3000));
        when(oTelTraceSourceConfig.getRetryInfo()).thenReturn(retryInfo);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(5000);

        httpServiceConfigurator.configure(serverBuilder, decoder, buffer);

        verify(serverBuilder).annotatedService(any(ArmeriaHttpService.class), any(HttpExceptionHandler.class));
    }
}
