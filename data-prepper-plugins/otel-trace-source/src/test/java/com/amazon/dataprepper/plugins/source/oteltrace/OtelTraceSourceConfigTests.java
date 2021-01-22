package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OtelTraceSourceConfigTests {
    private final String PLUGIN_NAME = "otel_trace_source";
    private final int DEFAULT_REQUEST_TIMEOUT = 10000;
    private final int DEFAULT_PORT = 21890;
    private final String TEST_KEY_CERT = "test.crt";
    private final String TEST_KEY = "test.key";

    @Test
    public void testDefault() {
        // Prepare
        final OTelTraceSourceConfig otelTraceSourceConfig =  OTelTraceSourceConfig.buildConfig(
                new PluginSetting(PLUGIN_NAME, new HashMap<>()));

        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertFalse(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.hasProtoReflectionService());
        assertFalse(otelTraceSourceConfig.isSsl());
        assertNull(otelTraceSourceConfig.getSslKeyCertChainFile());
        assertNull(otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfig() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PORT, true, true, true, TEST_KEY_CERT, TEST_KEY);

        // When
        final OTelTraceSourceConfig otelTraceSourceConfig =  OTelTraceSourceConfig.buildConfig(validPluginSetting);

        // Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertTrue(otelTraceSourceConfig.hasHealthCheck());
        assertTrue(otelTraceSourceConfig.hasProtoReflectionService());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertEquals(TEST_KEY_CERT, otelTraceSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    public void testInvalidConfig() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PORT, false, false, true, null, TEST_KEY);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslNullKeyCertPluginSetting));

        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PORT, false, false, true, "", TEST_KEY);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslEmptyKeyCertPluginSetting));

        // Prepare
        final PluginSetting sslNullKeyFilePluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PORT, false, false, true, TEST_KEY_CERT, null);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslNullKeyFilePluginSetting));

        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT, DEFAULT_PORT, false, false, true, TEST_KEY_CERT, "");
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslEmptyKeyFilePluginSetting));
    }

    private PluginSetting completePluginSettingForOtelTraceSource(final int requestTimeoutInMillis,
                                                                  final int port,
                                                                  final boolean healthCheck,
                                                                  final boolean protoReflectionService,
                                                                  final boolean isSSL,
                                                                  final String sslKeyCertChainFile,
                                                                  final String sslKeyFile) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTraceSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelTraceSourceConfig.PORT, port);
        settings.put(OTelTraceSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelTraceSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelTraceSourceConfig.SSL, isSSL);
        settings.put(OTelTraceSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelTraceSourceConfig.SSL_KEY_FILE, sslKeyFile);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
