/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

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

class OTLPSourceConfigTests {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
  private static final String PLUGIN_NAME = "otlp";
  private static final String TEST_KEY_CERT = "test.crt";
  private static final String TEST_KEY = "test.key";
  private static final String TEST_KEY_CERT_S3 = "s3://test.crt";
  private static final String TEST_KEY_S3 = "s3://test.key";
  private static final String TEST_REGION = "us-east-1";
  private static final int TEST_PORT = 45600;
  private static final int TEST_THREAD_COUNT = 888;
  private static final int TEST_MAX_CONNECTION_COUNT = 999;

  private static Stream<Arguments> provideCompressionOption() {
    return Stream.of(Arguments.of(CompressionOption.GZIP));
  }

  @Test
  void testDefault() {
    final OTLPSourceConfig config = new OTLPSourceConfig();

    assertEquals((int) Duration.ofSeconds(OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT).toMillis(),
        config.getRequestTimeoutInMillis());
    assertEquals(OTLPSourceConfig.DEFAULT_PORT, config.getPort());
    assertEquals(OTLPSourceConfig.DEFAULT_THREAD_COUNT, config.getThreadCount());
    assertEquals(OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, config.getMaxConnectionCount());
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
    settings.put(OTLPSourceConfig.COMPRESSION, compressionOption.name());

    final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertEquals(compressionOption, config.getCompression());
  }

  @Test
  void testValidConfigWithoutS3CertAndKey() {
    final PluginSetting validPluginSetting = completePluginSetting(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT, TEST_PORT,
        null, true, true, false, true,
        TEST_KEY_CERT, TEST_KEY, TEST_THREAD_COUNT, TEST_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(validPluginSetting.getSettings(),
        OTLPSourceConfig.class);
    config.validateAndInitializeCertAndKeyFileInS3();

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
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT, TEST_PORT, null, false, false, false,
        true, TEST_KEY_CERT_S3, TEST_KEY_S3, TEST_THREAD_COUNT, TEST_MAX_CONNECTION_COUNT);

    validPluginSettingWithS3CertAndKey.getSettings().put(OTLPSourceConfig.AWS_REGION, TEST_REGION);

    final OTLPSourceConfig config = OBJECT_MAPPER
        .convertValue(validPluginSettingWithS3CertAndKey.getSettings(), OTLPSourceConfig.class);
    config.validateAndInitializeCertAndKeyFileInS3();

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
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT, OTLPSourceConfig.DEFAULT_PORT,
        null, false, false, false, true, null, TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT, OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(sslNullKeyCertPluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
  }

  @Test
  void testRetryInfoConfig() {
    final PluginSetting pluginSetting = completePluginSetting(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT, OTLPSourceConfig.DEFAULT_PORT,
        null, false, false, false, true, TEST_KEY_CERT, "",
        OTLPSourceConfig.DEFAULT_THREAD_COUNT, OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThat(config.getRetryInfo().getMaxDelay(), equalTo(Duration.ofMillis(100)));
    assertThat(config.getRetryInfo().getMinDelay(), equalTo(Duration.ofMillis(50)));
  }

  @Test
  void testHttpHealthCheckWithUnframedRequestEnabled() {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(OTLPSourceConfig.ENABLE_UNFRAMED_REQUESTS, "true");
    settings.put(OTLPSourceConfig.HEALTH_CHECK_SERVICE, "true");
    settings.put(OTLPSourceConfig.PROTO_REFLECTION_SERVICE, "true");

    final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertTrue(config.hasHealthCheck());
    assertTrue(config.enableUnframedRequests());
    assertTrue(config.hasProtoReflectionService());
    assertTrue(config.enableHttpHealthCheck());
  }

  @Test
  void testHttpHealthCheckWithUnframedRequestDisabled() {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(OTLPSourceConfig.ENABLE_UNFRAMED_REQUESTS, "false");
    settings.put(OTLPSourceConfig.HEALTH_CHECK_SERVICE, "true");
    settings.put(OTLPSourceConfig.PROTO_REFLECTION_SERVICE, "true");

    final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertTrue(config.hasHealthCheck());
    assertFalse(config.enableUnframedRequests());
    assertTrue(config.hasProtoReflectionService());
    assertFalse(config.enableHttpHealthCheck());
  }

  @Test
  void testInvalidConfigWithEmptyKeyCert() {
    final PluginSetting sslEmptyKeyCertPluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        null,
        false,
        false,
        false,
        true,
        "",
        TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(sslEmptyKeyCertPluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
  }

  @Test
  void testInvalidConfigWithEmptyKeyFile() {
    final PluginSetting sslEmptyKeyFilePluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        null,
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        "",
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(sslEmptyKeyFilePluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThrows(IllegalArgumentException.class, config::validateAndInitializeCertAndKeyFileInS3);
  }

  @Test
  void testValidConfigWithCustomPath() {
    final String testPath = "/testPath";
    final PluginSetting customPathPluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        testPath,
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        "",
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThat(config.getLogsPath(), equalTo(testPath));
    assertThat(config.isLogsPathValid(), equalTo(true));
  }

  @Test
  void testInValidConfigWithCustomPath() {
    final String testPath = "invalidPath";
    final PluginSetting customPathPluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        testPath,
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        "",
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(customPathPluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertThat(config.getLogsPath(), equalTo(testPath));
    assertThat(config.isLogsPathValid(), equalTo(false));
  }

  @Test
  void testPathsAreDistinct() {
    final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        "/logs",
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
    pluginSetting.getSettings().put(OTLPSourceConfig.METRICS_PATH, "/metrics");
    pluginSetting.getSettings().put(OTLPSourceConfig.TRACES_PATH, "/traces");

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertTrue(config.arePathsDistinct());
  }

  @Test
  void testPathsAreNotDistinct() {
    final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        "/logs",
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
    pluginSetting.getSettings().put(OTLPSourceConfig.METRICS_PATH, "/logs");
    pluginSetting.getSettings().put(OTLPSourceConfig.TRACES_PATH, "/traces");

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertFalse(config.arePathsDistinct());
  }

  @Test
  void testValidPathsStartWithSlash() {
    final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        "/logs",
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
    pluginSetting.getSettings().put(OTLPSourceConfig.METRICS_PATH, "/metrics");
    pluginSetting.getSettings().put(OTLPSourceConfig.TRACES_PATH, "/traces");

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertTrue(config.isLogsPathValid());
    assertTrue(config.isMetricsPathValid());
    assertTrue(config.isTracesPathValid());
  }

  @Test
  void testInvalidPathsDoNotStartWithSlash() {
    final PluginSetting pluginSetting = completePluginSettingForOtelTelemetrySource(
        OTLPSourceConfig.DEFAULT_REQUEST_TIMEOUT,
        OTLPSourceConfig.DEFAULT_PORT,
        "logs",
        false,
        false,
        false,
        true,
        TEST_KEY_CERT,
        TEST_KEY,
        OTLPSourceConfig.DEFAULT_THREAD_COUNT,
        OTLPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT);
    pluginSetting.getSettings().put(OTLPSourceConfig.METRICS_PATH, "metrics");
    pluginSetting.getSettings().put(OTLPSourceConfig.TRACES_PATH, "traces");

    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
        OTLPSourceConfig.class);

    assertFalse(config.isLogsPathValid());
    assertFalse(config.isMetricsPathValid());
    assertFalse(config.isTracesPathValid());
  }

  @Test
  void testDefaultOutputFormats() {
    final OTLPSourceConfig config = new OTLPSourceConfig();

    assertEquals(OTelOutputFormat.OTEL, config.getLogsOutputFormat());
    assertEquals(OTelOutputFormat.OTEL, config.getMetricsOutputFormat());
    assertEquals(OTelOutputFormat.OTEL, config.getTracesOutputFormat());
  }

  @Test
  void testGenericOutputFormats() {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(OTLPSourceConfig.OUTPUT_FORMAT, OTelOutputFormat.OPENSEARCH.getFormatName());

    final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTLPSourceConfig.class);

    assertEquals(OTelOutputFormat.OPENSEARCH, config.getLogsOutputFormat());
    assertEquals(OTelOutputFormat.OPENSEARCH, config.getMetricsOutputFormat());
    assertEquals(OTelOutputFormat.OPENSEARCH, config.getTracesOutputFormat());
  }

  @Test
  void testCustomOutputFormats() {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(OTLPSourceConfig.LOGS_OUTPUT_FORMAT, OTelOutputFormat.OPENSEARCH.getFormatName());
    settings.put(OTLPSourceConfig.METRICS_OUTPUT_FORMAT, OTelOutputFormat.OPENSEARCH.getFormatName());
    settings.put(OTLPSourceConfig.TRACES_OUTPUT_FORMAT, OTelOutputFormat.OPENSEARCH.getFormatName());

    final PluginSetting pluginSetting = new PluginSetting(PLUGIN_NAME, settings);
    final OTLPSourceConfig config = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTLPSourceConfig.class);

    assertEquals(OTelOutputFormat.OPENSEARCH, config.getLogsOutputFormat());
    assertEquals(OTelOutputFormat.OPENSEARCH, config.getMetricsOutputFormat());
    assertEquals(OTelOutputFormat.OPENSEARCH, config.getTracesOutputFormat());
  }

  private PluginSetting completePluginSetting(final int requestTimeoutInMillis,
      final int port, final String path,
      final boolean healthCheck, final boolean protoReflectionService,
      final boolean enableUnframedRequests, final boolean isSSL,
      final String sslKeyCertChainFile, final String sslKeyFile,
      final int threadCount, final int maxConnectionCount) {
    final Map<String, Object> settings = new HashMap<>();
    settings.put(OTLPSourceConfig.PORT, port);
    settings.put(OTLPSourceConfig.LOGS_PATH, path);
    settings.put(OTLPSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
    settings.put(OTLPSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
    settings.put(OTLPSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
    settings.put(OTLPSourceConfig.SSL, isSSL);
    settings.put(OTLPSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
    settings.put(OTLPSourceConfig.SSL_KEY_FILE, sslKeyFile);
    settings.put(OTLPSourceConfig.THREAD_COUNT, threadCount);
    settings.put(OTLPSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
    settings.put(OTLPSourceConfig.RETRY_INFO,
        new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(100)));
    return new PluginSetting(PLUGIN_NAME, settings);
  }

  private PluginSetting completePluginSettingForOtelTelemetrySource(
      final int requestTimeoutInMillis,
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
    settings.put(OTLPSourceConfig.PORT, port);
    settings.put(OTLPSourceConfig.LOGS_PATH, path);
    settings.put(OTLPSourceConfig.HEALTH_CHECK_SERVICE, healthCheck);
    settings.put(OTLPSourceConfig.PROTO_REFLECTION_SERVICE, protoReflectionService);
    settings.put(OTLPSourceConfig.ENABLE_UNFRAMED_REQUESTS, enableUnframedRequests);
    settings.put(OTLPSourceConfig.SSL, isSSL);
    settings.put(OTLPSourceConfig.SSL_KEY_CERT_FILE, sslKeyCertChainFile);
    settings.put(OTLPSourceConfig.SSL_KEY_FILE, sslKeyFile);
    settings.put(OTLPSourceConfig.THREAD_COUNT, threadCount);
    settings.put(OTLPSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
    settings.put(OTLPSourceConfig.RETRY_INFO,
        new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(100)));
    return new PluginSetting(PLUGIN_NAME, settings);
  }
}
