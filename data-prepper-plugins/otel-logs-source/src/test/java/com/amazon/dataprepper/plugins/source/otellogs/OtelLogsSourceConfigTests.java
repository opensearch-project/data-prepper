/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otellogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.HashMap;
import java.util.Map;

import static com.amazon.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static com.amazon.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static com.amazon.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static com.amazon.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_THREAD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OtelLogsSourceConfigTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "otel_logs_source";
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
        final OTelLogsSourceConfig otelLogsSourceConfig = new OTelLogsSourceConfig();


        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT_MS, otelLogsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelLogsSourceConfig.getPort());
        assertEquals(DEFAULT_THREAD_COUNT, otelLogsSourceConfig.getThreadCount());
        assertEquals(OTelLogsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelLogsSourceConfig.getMaxConnectionCount());
        assertFalse(otelLogsSourceConfig.hasHealthCheck());
        assertFalse(otelLogsSourceConfig.hasProtoReflectionService());
        assertFalse(otelLogsSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelLogsSourceConfig.isSsl());
        assertNull(otelLogsSourceConfig.getSslKeyCertChainFile());
        assertNull(otelLogsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithoutS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelLogsSource(
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
        final OTelLogsSourceConfig otelLogsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelLogsSourceConfig.class);
        otelLogsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelLogsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelLogsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelLogsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelLogsSourceConfig.getMaxConnectionCount());
        assertTrue(otelLogsSourceConfig.hasHealthCheck());
        assertTrue(otelLogsSourceConfig.hasProtoReflectionService());
        assertTrue(otelLogsSourceConfig.isSsl());
        assertFalse(otelLogsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, otelLogsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelLogsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelLogsSource(
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

        validPluginSettingWithS3CertAndKey.getSettings().put(OTelLogsSourceConfig.AWS_REGION, TEST_REGION);

        final OTelLogsSourceConfig otelLogsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTelLogsSourceConfig.class);
        otelLogsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelLogsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelLogsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelLogsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelLogsSourceConfig.getMaxConnectionCount());
        assertFalse(otelLogsSourceConfig.hasHealthCheck());
        assertFalse(otelLogsSourceConfig.hasProtoReflectionService());
        assertTrue(otelLogsSourceConfig.isSsl());
        assertTrue(otelLogsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, otelLogsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, otelLogsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testInvalidConfigWithNullKeyCert() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT, false,
                false,
                false,
                true, null,
                TEST_KEY,
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);

        final OTelLogsSourceConfig otelLogsSourceConfig = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelLogsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelLogsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyCert() {
        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelLogsSource(
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

        final OTelLogsSourceConfig otelLogsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelLogsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelLogsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyFile() {
        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelLogsSource(
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

        final OTelLogsSourceConfig otelLogsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelLogsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelLogsSourceConfig::validateAndInitializeCertAndKeyFileInS3);
    }

    private PluginSetting completePluginSettingForOtelLogsSource(final int requestTimeoutInMillis,
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
        settings.put(OTelLogsSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelLogsSourceConfig.PORT, port);
        settings.put(OTelLogsSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelLogsSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelLogsSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelLogsSourceConfig.SSL, isSSL);
        settings.put(OTelLogsSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelLogsSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelLogsSourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelLogsSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
