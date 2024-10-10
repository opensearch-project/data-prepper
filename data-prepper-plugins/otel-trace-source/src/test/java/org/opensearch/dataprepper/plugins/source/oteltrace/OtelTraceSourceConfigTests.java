/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.Test;

import java.time.Duration;
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
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_THREAD_COUNT;

class OtelTraceSourceConfigTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
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

    private static Stream<Arguments> provideCompressionOption() {
        return Stream.of(Arguments.of(CompressionOption.GZIP));
    }

    @Test
    void testDefault() {

        // Prepare
        final OTelTraceSourceConfig otelTraceSourceConfig = new OTelTraceSourceConfig();


        // When/Then
        assertEquals(OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(OTelTraceSourceConfig.DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertEquals(OTelTraceSourceConfig.DEFAULT_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertEquals(CompressionOption.NONE, otelTraceSourceConfig.getCompression());
        assertFalse(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.hasProtoReflectionService());
        assertFalse(otelTraceSourceConfig.enableHttpHealthCheck());
        assertFalse(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertNull(otelTraceSourceConfig.getSslKeyCertChainFile());
        assertNull(otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestEnabled() {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTraceSourceConfig.ENABLE_UNFRAMED_REQUESTS, "true");
        settings.put(OTelTraceSourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelTraceSourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertTrue(otelTraceSourceConfig.hasHealthCheck());
        assertTrue(otelTraceSourceConfig.enableUnframedRequests());
        assertTrue(otelTraceSourceConfig.hasProtoReflectionService());
        assertTrue(otelTraceSourceConfig.enableHttpHealthCheck());
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestDisabled() {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTraceSourceConfig.ENABLE_UNFRAMED_REQUESTS, "false");
        settings.put(OTelTraceSourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelTraceSourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertTrue(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.enableUnframedRequests());
        assertTrue(otelTraceSourceConfig.hasProtoReflectionService());
        assertFalse(otelTraceSourceConfig.enableHttpHealthCheck());
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTraceSourceConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertEquals(compressionOption, otelTraceSourceConfig.getCompression());
    }

    @Test
    void testValidConfigWithoutS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSetting = completePluginSettingForOtelTraceSource(
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
        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        otelTraceSourceConfig.validateAndInitializeCertAndKeyFileInS3();

        // Then
        assertEquals(TEST_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, otelTraceSourceConfig.getPort());
        assertEquals(TEST_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertTrue(otelTraceSourceConfig.hasHealthCheck());
        assertTrue(otelTraceSourceConfig.hasProtoReflectionService());
        assertFalse(otelTraceSourceConfig.enableHttpHealthCheck());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertFalse(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, otelTraceSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    void testValidConfigWithS3CertAndKey() {
        // Prepare
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSettingForOtelTraceSource(
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
        assertFalse(otelTraceSourceConfig.enableHttpHealthCheck());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertTrue(otelTraceSourceConfig.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, otelTraceSourceConfig.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, otelTraceSourceConfig.getSslKeyFile());
    }

    @Test
    void testInvalidConfigWithNullKeyCert() {
        // Prepare
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    void testInvalidConfigWithEmptyKeyCert() {
        // Prepare
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);

    }

    @Test
    void testInvalidConfigWithEmptyKeyFile() {
        // Prepare
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThrows(IllegalArgumentException.class, otelTraceSourceConfig::validateAndInitializeCertAndKeyFileInS3);
    }

    @Test
    void testValidConfigWithCustomPath() {
        final String testPath = "/testPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThat(otelTraceSourceConfig.getPath(), equalTo(testPath));
        assertThat(otelTraceSourceConfig.isPathValid(), equalTo(true));
    }

    @Test
    void testInValidConfigWithCustomPath() {
        final String testPath = "invalidPath";
        // Prepare
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        // When/Then
        assertThat(otelTraceSourceConfig.getPath(), equalTo(testPath));
        assertThat(otelTraceSourceConfig.isPathValid(), equalTo(false));
    }

    @Test
    void testRetryInfoConfig() {
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelTraceSource(
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

        final OTelTraceSourceConfig otelTraceSourceConfig = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelTraceSourceConfig.class);


        assertThat(otelTraceSourceConfig.getRetryInfo().getMaxDelay(), equalTo(Duration.ofMillis(100)));
        assertThat(otelTraceSourceConfig.getRetryInfo().getMinDelay(), equalTo(Duration.ofMillis(50)));
    }

    private PluginSetting completePluginSettingForOtelTraceSource(final int requestTimeoutInMillis,
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
        settings.put(OTelTraceSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelTraceSourceConfig.PORT, port);
        settings.put(OTelTraceSourceConfig.PATH, path);
        settings.put(OTelTraceSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelTraceSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelTraceSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelTraceSourceConfig.SSL, isSSL);
        settings.put(OTelTraceSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelTraceSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelTraceSourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelTraceSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(OTelTraceSourceConfig.RETRY_INFO, new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(100)));
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
