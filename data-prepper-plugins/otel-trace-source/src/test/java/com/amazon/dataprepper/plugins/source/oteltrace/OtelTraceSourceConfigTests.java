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

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_THREAD_COUNT;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.SSL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OtelTraceSourceConfigTests {
    private static final String PLUGIN_NAME = "otel_trace_source";
    private static final String TEST_KEY_CERT = "test.crt";
    private static final String TEST_KEY = "test.key";
    private static final int TEST_REQUEST_TIMEOUT_MS = 777;
    private static final int TEST_PORT = 45600;
    private static final int TEST_THREAD_COUNT = 888;
    private static final int TEST_MAX_CONNECTION_COUNT = 999;

    @Test
    public void testDefault() {
        // Prepare
        final OTelTraceSourceConfig otelTraceSourceConfig = OTelTraceSourceConfig.buildConfig(
                new PluginSetting(PLUGIN_NAME, Collections.singletonMap(SSL, false)));

        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertEquals(DEFAULT_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(DEFAULT_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
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
                TEST_REQUEST_TIMEOUT_MS,
                TEST_PORT,
                true,
                true,
                false,
                true,
                TEST_KEY_CERT,
                TEST_KEY,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT);

        // When
        final OTelTraceSourceConfig otelTraceSourceConfig = OTelTraceSourceConfig.buildConfig(validPluginSetting);

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelTraceSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
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
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT, false,
                false,
                false,
                true, null,
                TEST_KEY,
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslNullKeyCertPluginSetting));

        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                false,
                false,
                false,
                true,
                "",
                TEST_KEY,
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslEmptyKeyCertPluginSetting));

        // Prepare
        final PluginSetting sslNullKeyFilePluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                null,
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslNullKeyFilePluginSetting));

        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelTraceSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);
        // When/Then
        assertThrows(IllegalArgumentException.class, () -> OTelTraceSourceConfig.buildConfig(sslEmptyKeyFilePluginSetting));
    }

    private PluginSetting completePluginSettingForOtelTraceSource(final int requestTimeoutInMillis,
                                                                  final int port,
                                                                  final boolean healthCheck,
                                                                  final boolean protoReflectionService,
                                                                  final boolean enableUnframedRequests,
                                                                  final boolean isSSL,
                                                                  final String sslKeyCertChainFile,
                                                                  final String sslKeyFile,
                                                                  final int threadCount,
                                                                  final int maxConnectionCount) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTraceSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelTraceSourceConfig.PORT, port);
        settings.put(OTelTraceSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelTraceSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelTraceSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelTraceSourceConfig.SSL, isSSL);
        settings.put(OTelTraceSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelTraceSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelTraceSourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelTraceSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
