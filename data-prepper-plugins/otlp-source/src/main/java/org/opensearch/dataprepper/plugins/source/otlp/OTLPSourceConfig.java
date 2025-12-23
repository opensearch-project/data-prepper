/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otlp;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;

public class OTLPSourceConfig {
  static final String REQUEST_TIMEOUT = "request_timeout";
  static final String PORT = "port";
  static final String LOGS_PATH = "logs_path";
  static final String METRICS_PATH = "metrics_path";
  static final String TRACES_PATH = "traces_path";
  static final String SSL = "ssl";
  static final String OUTPUT_FORMAT = "output_format";
  static final String LOGS_OUTPUT_FORMAT = "logs_output_format";
  static final String METRICS_OUTPUT_FORMAT = "metrics_output_format";
  static final String TRACES_OUTPUT_FORMAT = "traces_output_format";
  static final String USE_ACM_CERT_FOR_SSL = "use_acm_certificate_for_ssl";
  static final String ACM_CERT_ISSUE_TIME_OUT_MILLIS = "acm_certificate_timeout";
  static final String HEALTH_CHECK_SERVICE = "health_check_service";
  static final String PROTO_REFLECTION_SERVICE = "proto_reflection_service";
  static final String SSL_KEY_CERT_FILE = "ssl_certificate_file";
  static final String SSL_KEY_FILE = "ssl_key_file";
  static final String ACM_CERT_ARN = "acm_certificate_arn";
  static final String ACM_PRIVATE_KEY_PASSWORD = "acm_private_key_password";
  static final String AWS_REGION = "aws_region";
  static final String THREAD_COUNT = "thread_count";
  static final String MAX_CONNECTION_COUNT = "max_connection_count";
  static final String ENABLE_UNFRAMED_REQUESTS = "unframed_requests";
  static final String COMPRESSION = "compression";
  static final String RETRY_INFO = "retry_info";
  static final int DEFAULT_REQUEST_TIMEOUT = 10; // in seconds
  static final int DEFAULT_PORT = 21893;
  static final int DEFAULT_THREAD_COUNT = 200;
  static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
  static final boolean DEFAULT_SSL = true;
  static final boolean DEFAULT_ENABLED_UNFRAMED_REQUESTS = false;
  static final boolean DEFAULT_HEALTH_CHECK = false;
  static final boolean DEFAULT_PROTO_REFLECTION_SERVICE = false;
  static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
  static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT = 120; // in seconds
  private static final String S3_PREFIX = "s3://";
  static final String UNAUTHENTICATED_HEALTH_CHECK = "unauthenticated_health_check";

  @JsonProperty(REQUEST_TIMEOUT)
  @DurationMin(seconds = 5)
  @DurationMax(seconds = 3600)
  private Duration requestTimeout = Duration.ofSeconds(DEFAULT_REQUEST_TIMEOUT);

  @JsonProperty(PORT)
  private int port = DEFAULT_PORT;

  @JsonProperty(LOGS_PATH)
  @Size(min = 1, message = "logsPath length should be at least 1")
  private String logsPath;

  @JsonProperty(METRICS_PATH)
  @Size(min = 1, message = "metricsPath length should be at least 1")
  private String metricsPath;

  @JsonProperty(TRACES_PATH)
  @Size(min = 1, message = "tracesPath length should be at least 1")
  private String tracesPath;

  @JsonProperty(HEALTH_CHECK_SERVICE)
  private boolean healthCheck = DEFAULT_HEALTH_CHECK;

  @JsonProperty(PROTO_REFLECTION_SERVICE)
  private boolean protoReflectionService = DEFAULT_PROTO_REFLECTION_SERVICE;

  @JsonProperty(ENABLE_UNFRAMED_REQUESTS)
  private boolean enableUnframedRequests = DEFAULT_ENABLED_UNFRAMED_REQUESTS;

  @JsonProperty(SSL)
  private boolean ssl = DEFAULT_SSL;

  @JsonProperty(OUTPUT_FORMAT)
  private OTelOutputFormat outputFormat = OTelOutputFormat.OTEL;

  @JsonProperty(LOGS_OUTPUT_FORMAT)
  private OTelOutputFormat logsOutputFormat = null;

  @JsonProperty(METRICS_OUTPUT_FORMAT)
  private OTelOutputFormat metricsOutputFormat = null;

  @JsonProperty(TRACES_OUTPUT_FORMAT)
  private OTelOutputFormat tracesOutputFormat = null;

  @JsonProperty(USE_ACM_CERT_FOR_SSL)
  private boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

  @JsonProperty(ACM_CERT_ISSUE_TIME_OUT_MILLIS)
  @DurationMin(seconds = 5)
  @DurationMax(seconds = 3600)
  private Duration acmCertIssueTimeOutMillis = Duration.ofSeconds(DEFAULT_ACM_CERT_ISSUE_TIME_OUT);

  @JsonProperty(SSL_KEY_CERT_FILE)
  private String sslKeyCertChainFile;

  @JsonProperty(SSL_KEY_FILE)
  private String sslKeyFile;

  private boolean sslCertAndKeyFileInS3;

  @JsonProperty(ACM_CERT_ARN)
  private String acmCertificateArn;

  @JsonProperty(ACM_PRIVATE_KEY_PASSWORD)
  private String acmPrivateKeyPassword;

  @JsonProperty(AWS_REGION)
  private String awsRegion;

  @JsonProperty(THREAD_COUNT)
  private int threadCount = DEFAULT_THREAD_COUNT;

  @JsonProperty(MAX_CONNECTION_COUNT)
  private int maxConnectionCount = DEFAULT_MAX_CONNECTION_COUNT;

  @JsonProperty("authentication")
  private PluginModel authentication;

  @JsonProperty(UNAUTHENTICATED_HEALTH_CHECK)
  private boolean unauthenticatedHealthCheck = false;

  @JsonProperty(COMPRESSION)
  private CompressionOption compression = CompressionOption.NONE;

  @JsonProperty("max_request_length")
  private ByteCount maxRequestLength;

  @JsonProperty(RETRY_INFO)
  private RetryInfoConfig retryInfo;

  @AssertTrue(message = LOGS_PATH + " should start with /")
  boolean isLogsPathValid() {
    return logsPath == null || logsPath.startsWith("/");
  }

  @AssertTrue(message = METRICS_PATH + " should start with /")
  boolean isMetricsPathValid() {
    return metricsPath == null || metricsPath.startsWith("/");
  }

  @AssertTrue(message = TRACES_PATH + " should start with /")
  boolean isTracesPathValid() {
    return tracesPath == null || tracesPath.startsWith("/");
  }

  @AssertTrue(message = LOGS_PATH + ", " + METRICS_PATH + ", and " + TRACES_PATH + " should be distinct")
  boolean arePathsDistinct() {
    if (logsPath == null || metricsPath == null || tracesPath == null) {
      return true; // Validation is not applicable if any of the paths are null
    }
    return !logsPath.equals(metricsPath) && !logsPath.equals(tracesPath) && !metricsPath.equals(tracesPath);
  }

  public void validateAndInitializeCertAndKeyFileInS3() {
    boolean certAndKeyFileInS3 = false;
    if (useAcmCertForSSL) {
      validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), acmCertificateArn, ACM_CERT_ARN);
      validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), awsRegion, AWS_REGION);
    } else if (ssl) {
      validateSSLCertificateFiles();
      certAndKeyFileInS3 = isSSLCertificateLocatedInS3();
      if (certAndKeyFileInS3) {
        validateSSLArgument("The certificate and key files are located in S3", awsRegion, AWS_REGION);
      }
    }
    sslCertAndKeyFileInS3 = certAndKeyFileInS3;
  }

  private void validateSSLArgument(final String sslTypeMessage, final String argument, final String argumentName) {
    if (StringUtils.isEmpty(argument)) {
      throw new IllegalArgumentException(
          String.format("%s, %s can not be empty or null", sslTypeMessage, argumentName));
    }
  }

  private void validateSSLCertificateFiles() {
    validateSSLArgument(String.format("%s is enabled", SSL), sslKeyCertChainFile, SSL_KEY_CERT_FILE);
    validateSSLArgument(String.format("%s is enabled", SSL), sslKeyFile, SSL_KEY_FILE);
  }

  private boolean isSSLCertificateLocatedInS3() {
    return sslKeyCertChainFile.toLowerCase().startsWith(S3_PREFIX) &&
        sslKeyFile.toLowerCase().startsWith(S3_PREFIX);
  }

  /**
   * Note: The value is cast to int since the maximum allowed duration
   * (1 hour = 3,600,000 ms) is well within the int range.
   * Validation via @DurationMax ensures this is safe.
   * Casting to int is necessary because ServerConfiguration method signature
   * requires it.
   * @return the request timeout in milliseconds as an integer
   */
  public int getRequestTimeoutInMillis() {
    return (int) requestTimeout.toMillis();
  }

  public OTelOutputFormat getLogsOutputFormat() {
    return logsOutputFormat != null ? logsOutputFormat : outputFormat;
  }

  public OTelOutputFormat getMetricsOutputFormat() {
    return metricsOutputFormat != null ? metricsOutputFormat : outputFormat;
  }

  public OTelOutputFormat getTracesOutputFormat() {
    return tracesOutputFormat != null ? tracesOutputFormat : outputFormat;
  }

  public int getPort() {
    return port;
  }

  public String getLogsPath() {
    return logsPath;
  }

  public String getMetricsPath() {
    return metricsPath;
  }

  public String getTracesPath() {
    return tracesPath;
  }

  public boolean hasHealthCheck() {
    return healthCheck;
  }

  public boolean enableHttpHealthCheck() {
    return enableUnframedRequests() && hasHealthCheck();
  }

  public boolean hasProtoReflectionService() {
    return protoReflectionService;
  }

  public boolean enableUnframedRequests() {
    return enableUnframedRequests;
  }

  public boolean isSsl() {
    return ssl;
  }

  public boolean useAcmCertForSSL() {
    return useAcmCertForSSL;
  }

  public long getAcmCertIssueTimeOutMillis() {
    return acmCertIssueTimeOutMillis.toMillis();
  }

  public String getSslKeyCertChainFile() {
    return sslKeyCertChainFile;
  }

  public String getSslKeyFile() {
    return sslKeyFile;
  }

  public String getAcmCertificateArn() {
    return acmCertificateArn;
  }

  public String getAcmPrivateKeyPassword() {
    return acmPrivateKeyPassword;
  }

  public boolean isSslCertAndKeyFileInS3() {
    return sslCertAndKeyFileInS3;
  }

  public String getAwsRegion() {
    return awsRegion;
  }

  public int getThreadCount() {
    return threadCount;
  }

  public int getMaxConnectionCount() {
    return maxConnectionCount;
  }

  public PluginModel getAuthentication() {
    return authentication;
  }

  public boolean isUnauthenticatedHealthCheck() {
    return unauthenticatedHealthCheck;
  }

  public CompressionOption getCompression() {
    return compression;
  }

  public ByteCount getMaxRequestLength() {
    return maxRequestLength;
  }

  public RetryInfoConfig getRetryInfo() {
    return retryInfo;
  }

}
