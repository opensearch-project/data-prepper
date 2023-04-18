/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.AWS_REGION;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.DEFAULT_THREAD_COUNT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.ENABLE_UNFRAMED_REQUESTS;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.HEALTH_CHECK_SERVICE;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.MAX_CONNECTION_COUNT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.PATH;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.PORT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.PROTO_REFLECTION_SERVICE;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.REQUEST_TIMEOUT;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.SSL;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.SSL_KEY_CERT_FILE;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.SSL_KEY_FILE;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.THREAD_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

class OtelLogsSourceConfigTests {
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
    void testDefault() {

        // Prepare
        final OTelLogsSourceConfig otelLogsSourceConfig = new OTelLogsSourceConfig();


        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT_MS, otelLogsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelLogsSourceConfig.getPort());
        assertEquals(DEFAULT_THREAD_COUNT, otelLogsSourceConfig.getThreadCount());
        assertEquals(DEFAULT_MAX_CONNECTION_COUNT, otelLogsSourceConfig.getMaxConnectionCount());
        assertFalse(otelLogsSourceConfig.hasHealthCheck());
        assertFalse(otelLogsSourceConfig.hasProtoReflectionService());
        assertFalse(otelLogsSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelLogsSourceConfig.isSsl());
        assertNull(otelLogsSourceConfig.getSslKeyCertChainFile());
        assertNull(otelLogsSourceConfig.getSslKeyFile());
    }

    @Test
    void testValidConfigWithoutS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelLogsSource(
                TEST_REQUEST_TIMEOUT_MS,
                TEST_PORT,
                null,
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
    void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelLogsSource(
                TEST_REQUEST_TIMEOUT_MS,
                TEST_PORT,
                null,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT_S3,
                TEST_KEY_S3,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT);

        validPluginSettingWithS3CertAndKey.getSettings().put(AWS_REGION, TEST_REGION);

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
    void testInvalidConfigWithNullKeyCert() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                null,
                false,
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
    void testInvalidConfigWithEmptyKeyCert() {
        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                null,
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
    void testInvalidConfigWithEmptyKeyFile() {
        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                null,
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

    @Test
    void testValidConfigWithCustomPath() {
        final String testPath = "/testPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                testPath,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);

        final OTelLogsSourceConfig oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelLogsSourceConfig.class);

        // When/Then
        assertThat(oTelLogsSourceConfig.getPath(), equalTo(testPath));
        assertThat(oTelLogsSourceConfig.isPathValid(), equalTo(true));
    }

    @Test
    void testInValidConfigWithCustomPath() {
        final String testPath = "invalidPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelLogsSource(
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_PORT,
                testPath,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                DEFAULT_THREAD_COUNT,
                DEFAULT_MAX_CONNECTION_COUNT);

        final OTelLogsSourceConfig oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelLogsSourceConfig.class);

        // When/Then
        assertThat(oTelLogsSourceConfig.getPath(), equalTo(testPath));
        assertThat(oTelLogsSourceConfig.isPathValid(), equalTo(false));
    }

    private PluginSetting completePluginSettingForOtelLogsSource(final int requestTimeoutInMillis,
                                                                 final int port,
                                                                 final String path,
                                                                 final boolean healthCheck,
                                                                 final boolean protoReflectionService,
                                                                 final boolean enableUnframedRequests,
                                                                 final boolean isSSL,
                                                                 final String sslKeyCertChainFile,
                                                                 final String sslKeyFile,
                                                                 final int threadCount,
                                                                 final int maxConnectionCount) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(PORT, port);
        settings.put(PATH, path);
        settings.put(HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(SSL, isSSL);
        settings.put(SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(SSL_KEY_FILE, sslKeyFile);
        settings.put(THREAD_COUNT, threadCount);
        settings.put(MAX_CONNECTION_COUNT, maxConnectionCount);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
