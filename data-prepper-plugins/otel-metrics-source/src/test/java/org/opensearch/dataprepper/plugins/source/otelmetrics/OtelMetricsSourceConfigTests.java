/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_THREAD_COUNT;

class OtelMetricsSourceConfigTests {
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

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {

        // Prepare
        final OTelMetricsSourceConfig otelMetricsSourceConfig = new OTelMetricsSourceConfig();


        // When/Then
        assertEquals(DEFAULT_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(DEFAULT_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(DEFAULT_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(OTelMetricsSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertEquals(CompressionOption.NONE, otelMetricsSourceConfig.getCompression());
        assertFalse(otelMetricsSourceConfig.hasHealthCheck());
        assertFalse(otelMetricsSourceConfig.enableHttpHealthCheck());
        assertFalse(otelMetricsSourceConfig.hasProtoReflectionService());
        assertFalse(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertNull(otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertNull(otelMetricsSourceConfig.getSslKeyFile());
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelMetricsSourceConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelMetricsSourceConfig oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(
                pluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertEquals(compressionOption, oTelMetricsSourceConfig.getCompression());
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestEnabled() {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelMetricsSourceConfig.ENABLE_UNFRAMED_REQUESTS, "true");
        settings.put(OTelMetricsSourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelMetricsSourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertTrue(otelMetricsSourceConfig.hasHealthCheck());
        assertTrue(otelMetricsSourceConfig.enableUnframedRequests());
        assertTrue(otelMetricsSourceConfig.hasProtoReflectionService());
        assertTrue(otelMetricsSourceConfig.enableHttpHealthCheck());
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestDisabled() {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelMetricsSourceConfig.ENABLE_UNFRAMED_REQUESTS, "false");
        settings.put(OTelMetricsSourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelMetricsSourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertTrue(otelMetricsSourceConfig.hasHealthCheck());
        assertFalse(otelMetricsSourceConfig.enableUnframedRequests());
        assertTrue(otelMetricsSourceConfig.hasProtoReflectionService());
        assertFalse(otelMetricsSourceConfig.enableHttpHealthCheck());
    }

    @Test
    void testValidConfigWithoutS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelMetricsSource(
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
        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
        otelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertTrue(otelMetricsSourceConfig.hasHealthCheck());
        assertTrue(otelMetricsSourceConfig.hasProtoReflectionService());
        assertFalse(otelMetricsSourceConfig.enableHttpHealthCheck());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertFalse(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelMetricsSourceConfig.getSslKeyFile());
    }

    @Test
    void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelMetricsSource(
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

        validPluginSettingWithS3CertAndKey.getSettings().put(OTelMetricsSourceConfig.AWS_REGION, TEST_REGION);

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTelMetricsSourceConfig.class);
        otelMetricsSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelMetricsSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelMetricsSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelMetricsSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelMetricsSourceConfig.getMaxConnectionCount());
        assertFalse(otelMetricsSourceConfig.hasHealthCheck());
        assertFalse(otelMetricsSourceConfig.enableHttpHealthCheck());
        assertFalse(otelMetricsSourceConfig.hasProtoReflectionService());
        assertTrue(otelMetricsSourceConfig.isSsl());
        assertTrue(otelMetricsSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, otelMetricsSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, otelMetricsSourceConfig.getSslKeyFile());
    }

    @Test
    void testInvalidConfigWithNullKeyCert() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    void testInvalidConfigWithEmptyKeyCert() {
        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    void testInvalidConfigWithEmptyKeyFile() {
        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelMetricsSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelMetricsSourceConfig::validateAndInitializeCertAndKeyFileInS3);
    }

    @Test
    void testValidConfigWithCustomPath() {
        final String testPath = "/testPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThat(oTelMetricsSourceConfig.getPath(), equalTo(testPath));
        assertThat(oTelMetricsSourceConfig.isPathValid(), equalTo(true));
    }

    @Test
    void testInValidConfigWithCustomPath() {
        final String testPath = "invalidPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelMetricsSourceConfig.class);

        // When/Then
        assertThat(oTelMetricsSourceConfig.getPath(), equalTo(testPath));
        assertThat(oTelMetricsSourceConfig.isPathValid(), equalTo(false));
    }

    @Test
    void testRetryInfoConfig() {
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelMetricsSource(
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

        final OTelMetricsSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelMetricsSourceConfig.class);


        RetryInfoConfig retryInfo = otelTraceSourceConfig.getRetryInfo();
        assertThat(retryInfo.getMaxDelay(), equalTo(100));
        assertThat(retryInfo.getMinDelay(), equalTo(50));
    }

    private PluginSetting completePluginSettingForOtelMetricsSource(final int requestTimeoutInMillis,
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
        settings.put(OTelMetricsSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelMetricsSourceConfig.PORT, port);
        settings.put(OTelMetricsSourceConfig.PATH, path);
        settings.put(OTelMetricsSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelMetricsSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelMetricsSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelMetricsSourceConfig.SSL, isSSL);
        settings.put(OTelMetricsSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelMetricsSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelMetricsSourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelMetricsSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(OTelMetricsSourceConfig.RETRY_INFO, new RetryInfoConfig(50, 100));
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
