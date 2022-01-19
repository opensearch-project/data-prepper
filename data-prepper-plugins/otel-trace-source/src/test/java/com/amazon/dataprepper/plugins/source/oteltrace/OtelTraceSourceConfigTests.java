/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static com.amazon.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_THREAD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OtelTraceSourceConfigTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "otel_trace_source";
    private static final String TEST_KEY_CERT = "test.crt";
    private static final String TEST_KEY = "test.key";
    private static final String TEST_KEY_CERT_S3 = "s3://test.crt";
    private static final String TEST_KEY_S3 = "s3://test.key";
    private static final String TEST_REGION = "us-east-1";
    private static final int TEST_REQUEST_TIMEOUT_MS = 777;
    private static final int TEST_PORT = 45600;
    private static final int TEST_THREAD_COUNT = 888;
    private static final int TEST_MAX_CONNECTION_COUNT = 999;

    @Test
    public void testDefault() {

        // Prepare
        final OTelTraceSourceConfig otelTraceSourceConfig = new OTelTraceSourceConfig();


        // When/Then
        assertEquals(OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(OTelTraceSourceConfig.DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertEquals(OTelTraceSourceConfig.DEFAULT_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertFalse(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.hasProtoReflectionService());
        assertFalse(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertNull(otelTraceSourceConfig.getSslKeyCertChainFile());
        assertNull(otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithoutS3CertAndKey() {
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
        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        otelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelTraceSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertTrue(otelTraceSourceConfig.hasHealthCheck());
        assertTrue(otelTraceSourceConfig.hasProtoReflectionService());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertFalse(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, otelTraceSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelTraceSource(
                TEST_REQUEST_TIMEOUT_MS,
                TEST_PORT,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT_S3,
                TEST_KEY_S3,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT);

        validPluginSettingWithS3CertAndKey.getSettings().put(OTelTraceSourceConfig.AWS_REGION, TEST_REGION);

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTelTraceSourceConfig.class);
        otelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelTraceSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertFalse(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.hasProtoReflectionService());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertTrue(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, otelTraceSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    public void testInvalidConfigWithNullKeyCert() {
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyCert() {
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyFile() {
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);
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
