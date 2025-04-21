/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

class OtelTelemetrySourceConfigTests {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String PLUGIN_NAME = "otel_telemetry_source";
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
        final OTelTelemetrySourceConfig config = new OTelTelemetrySourceConfig();

        assertEquals(OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, config.getRequestTimeoutInMillis());
        assertEquals(OTelTelemetrySourceConfig.DEFAULT_PORT, config.getPort());
        assertEquals(OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT, config.getThreadCount());
        assertEquals(OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT, config.getMaxConnectionCount());
        assertEquals(CompressionOption.NONE, config.getCompression());
        assertFalse(config.hasHealthCheck());
        assertFalse(config.hasProtoReflectionService());
        assertFalse(config.enableHttpHealthCheck());
        assertFalse(config.isSslCertAndKeyFileInS3());
        assertTrue(config.isSsl());
        assertNull(config.getSslKeyCertChainFile());
        assertNull(config.getSslKeyFile());
    }

    @ParameterizedTest
    @MethodSource("provideCompressionOption")
    void testValidCompression(final CompressionOption compressionOption) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTelemetrySourceConfig.COMPRESSION, compressionOption.name());

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertEquals(compressionOption, config.getCompression());
    }

    @Test
    void testValidConfigWithoutS3CertAndKey() {
        final PluginSetting validPluginSetting = completePluginSetting(
                TEST_REQUEST_TIMEOUT_MS, TEST_PORT, null, true, true, false, true,
                TEST_KEY_CERT, TEST_KEY, TEST_THREAD_COUNT, TEST_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(), OTelTelemetrySourceConfig.class);
        config.validateAndInitializeCertAndKeyFileInS3();

        assertEquals(TEST_REQUEST_TIMEOUT_MS, config.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, config.getPort());
        assertEquals(TEST_THREAD_COUNT, config.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, config.getMaxConnectionCount());
        assertTrue(config.hasHealthCheck());
        assertTrue(config.hasProtoReflectionService());
        assertFalse(config.enableHttpHealthCheck());
        assertTrue(config.isSsl());
        assertFalse(config.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT, config.getSslKeyCertChainFile());
        assertEquals(TEST_KEY, config.getSslKeyFile());
    }

    @Test
    void testValidConfigWithS3CertAndKey() {
        final PluginSetting validPluginSettingWithS3CertAndKey = completePluginSetting(
                TEST_REQUEST_TIMEOUT_MS, TEST_PORT, null, false, false, false, true,
                TEST_KEY_CERT_S3, TEST_KEY_S3, TEST_THREAD_COUNT, TEST_MAX_CONNECTION_COUNT);

        validPluginSettingWithS3CertAndKey.getSettings().put(OTelTelemetrySourceConfig.AWS_REGION, TEST_REGION);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTelTelemetrySourceConfig.class);
        config.validateAndInitializeCertAndKeyFileInS3();

        assertEquals(TEST_REQUEST_TIMEOUT_MS, config.getRequestTimeoutInMillis());
        assertEquals(TEST_PORT, config.getPort());
        assertEquals(TEST_THREAD_COUNT, config.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, config.getMaxConnectionCount());
        assertFalse(config.hasHealthCheck());
        assertFalse(config.hasProtoReflectionService());
        assertFalse(config.enableHttpHealthCheck());
        assertTrue(config.isSsl());
        assertTrue(config.isSslCertAndKeyFileInS3());
        assertEquals(TEST_KEY_CERT_S3, config.getSslKeyCertChainFile());
        assertEquals(TEST_KEY_S3, config.getSslKeyFile());
    }

    @Test
    void testInvalidConfigWithNullKeyCert() {
        final PluginSetting sslNullKeyCertPluginSetting = completePluginSetting(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, OTelTelemetrySourceConfig.DEFAULT_PORT,
                null, false, false, false, true, null, TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT, OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
    }

    @Test
    void testRetryInfoConfig() {
        final PluginSetting pluginSetting = completePluginSetting(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, OTelTelemetrySourceConfig.DEFAULT_PORT,
                null, false, false, false, true, TEST_KEY_CERT, "",
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT, OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThat(config.getRetryInfo().getMaxDelay(), equalTo(Duration.ofMillis(100)));
        assertThat(config.getRetryInfo().getMinDelay(), equalTo(Duration.ofMillis(50)));
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestEnabled() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTelemetrySourceConfig.ENABLE_UNFRAMED_REQUESTS, "true");
        settings.put(OTelTelemetrySourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelTelemetrySourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertTrue(config.hasHealthCheck());
        assertTrue(config.enableUnframedRequests());
        assertTrue(config.hasProtoReflectionService());
        assertTrue(config.enableHttpHealthCheck());
    }

    @Test
    void testHttpHealthCheckWithUnframedRequestDisabled() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTelemetrySourceConfig.ENABLE_UNFRAMED_REQUESTS, "false");
        settings.put(OTelTelemetrySourceConfig.HEALTH_CHECK_SERVICE, "true");
        settings.put(OTelTelemetrySourceConfig.PROTO_REFLECTION_SERVICE, "true");

        final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertTrue(config.hasHealthCheck());
        assertFalse(config.enableUnframedRequests());
        assertTrue(config.hasProtoReflectionService());
        assertFalse(config.enableHttpHealthCheck());
    }

    @Test
    void testInvalidConfigWithEmptyKeyCert() {
        final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                null,
                false,
                false,
                false,
                true,
                "",
                TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
    }

    @Test
    void testInvalidConfigWithEmptyKeyFile() {
        final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                null,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
    }

    @Test
    void testValidConfigWithCustomPath() {
        final String testPath = "/testPath";
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                testPath,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThat(config.getLogsPath(), equalTo(testPath));
        assertThat(config.isLogsPathValid(), equalTo(true));
    }

    @Test
    void testInValidConfigWithCustomPath() {
        final String testPath = "invalidPath";
        final PluginSetting customPathPluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                testPath,
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                "",
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertThat(config.getLogsPath(), equalTo(testPath));
        assertThat(config.isLogsPathValid(), equalTo(false));
    }

    @Test
    void testPathsAreDistinct() {
        final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                "/logs",
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.METRICS_PATH, "/metrics");
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.TRACES_PATH, "/traces");

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertTrue(config.arePathsDistinct());
    }

    @Test
    void testPathsAreNotDistinct() {
        final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                "/logs",
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.METRICS_PATH, "/logs");
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.TRACES_PATH, "/traces");

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertFalse(config.arePathsDistinct());
    }

    @Test
    void testValidPathsStartWithSlash() {
        final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                "/logs",
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.METRICS_PATH, "/metrics");
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.TRACES_PATH, "/traces");

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertTrue(config.isLogsPathValid());
        assertTrue(config.isMetricsPathValid());
        assertTrue(config.isTracesPathValid());
    }

    @Test
    void testInvalidPathsDoNotStartWithSlash() {
        final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
                OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS,
                OTelTelemetrySourceConfig.DEFAULT_PORT,
                "logs",
                false,
                false,
                false,
                true,
                TEST_KEY_CERT,
                TEST_KEY,
                OTelTelemetrySourceConfig.DEFAULT_THREAD_COUNT,
                OTelTelemetrySourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.METRICS_PATH, "metrics");
        pluginSetting.getSettings().put(OTelTelemetrySourceConfig.TRACES_PATH, "traces");

        final OTelTelemetrySourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTelemetrySourceConfig.class);

        assertFalse(config.isLogsPathValid());
        assertFalse(config.isMetricsPathValid());
        assertFalse(config.isTracesPathValid());
    }

    private PluginSetting completePluginSetting(final int requestTimeoutInMillis, final int port, final String path,
                                                final boolean healthCheck, final boolean protoReflectionService,
                                                final boolean enableUnframedRequests, final boolean isSSL,
                                                final String sslKeyCertChainFile, final String sslKeyFile,
                                                final int threadCount, final int maxConnectionCount) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(OTelTelemetrySourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelTelemetrySourceConfig.PORT, port);
        settings.put(OTelTelemetrySourceConfig.LOGS_PATH, path);
        settings.put(OTelTelemetrySourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelTelemetrySourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelTelemetrySourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelTelemetrySourceConfig.SSL, isSSL);
        settings.put(OTelTelemetrySourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelTelemetrySourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelTelemetrySourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelTelemetrySourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(OTelTelemetrySourceConfig.RETRY_INFO, new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(100)));
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    private PluginSetting completePluginSettingForOtelTelemetrySource(final int requestTimeoutInMillis,
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
        settings.put(OTelTelemetrySourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(OTelTelemetrySourceConfig.PORT, port);
        settings.put(OTelTelemetrySourceConfig.LOGS_PATH, path);
        settings.put(OTelTelemetrySourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
        settings.put(OTelTelemetrySourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
        settings.put(OTelTelemetrySourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
        settings.put(OTelTelemetrySourceConfig.SSL, isSSL);
        settings.put(OTelTelemetrySourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
        settings.put(OTelTelemetrySourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(OTelTelemetrySourceConfig.THREAD_COUNT, threadCount);
        settings.put(OTelTelemetrySourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(OTelTelemetrySourceConfig.RETRY_INFO, new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(100)));
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}
