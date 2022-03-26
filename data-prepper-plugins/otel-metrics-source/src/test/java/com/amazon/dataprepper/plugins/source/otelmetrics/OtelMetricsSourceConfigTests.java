/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazon.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static com.amazon.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_PORT;
import static com.amazon.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static com.amazon.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_THREAD_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class OtelMetricsSourceConfigTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PLUGIN_NAME = "otel_metrics_source";
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
        final OTelMetricsSourceConfig otelMetricsSourceConfig = new OTelMetricsSourceConfig();


        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(DEFAULT_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(OTelMetricsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertFalse(otelMetricsSourceConfig.hasHealthCheck());
        assertFalse(otelMetricsSourceConfig.hasProtoReflectionService());
        assertFalse(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertNull(otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertNull(otelMetricsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithoutS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelMetricsSource(
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
        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
        otelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertTrue(otelMetricsSourceConfig.hasHealthCheck());
        assertTrue(otelMetricsSourceConfig.hasProtoReflectionService());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertFalse(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelMetricsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelMetricsSource(
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

        validPluginSettingWithS3CertAndKey.getSettings().put(OTelMetricsSourceConfig.AWS_REGION, TEST_REGION);

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTelMetricsSourceConfig.class);
        otelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertFalse(otelMetricsSourceConfig.hasHealthCheck());
        assertFalse(otelMetricsSourceConfig.hasProtoReflectionService());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertTrue(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, otelMetricsSourceConfig.getSslKeyFile());
    }

    @Test
    public void testInvalidConfigWithNullKeyCert() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelMetricsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT, false,
                false,
                false,
                true, null,
                TEST_KEY,
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyCert() {
        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    public void testInvalidConfigWithEmptyKeyFile() {
        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);
    }

    private PluginSetting completePluginSettingForOtelMetricsSource(final int requestTimeoutInMillis,
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
        settings.put(OTelMetricsSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelMetricsSourceConfig.PORT, port);
        settings.put(OTelMetricsSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelMetricsSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelMetricsSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelMetricsSourceConfig.SSL, isSSL);
        settings.put(OTelMetricsSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelMetricsSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelMetricsSourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelMetricsSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
