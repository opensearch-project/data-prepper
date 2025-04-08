/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;

public class OTelMetricsSourceConfig {
    static final String REQUEST_TIMEOUT = "request_timeout";
    static final String PORT = "port";
    static final String PATH = "path";
    static final String SSL = "ssl";
    static final String USE_ACM_CERT_FOR_SSL = "useAcmCertForSSL";
    static final String ACM_CERT_ISSUE_TIME_OUT_MILLIS = "acmCertIssueTimeOutMillis";
    static final String HEALTH_CHECK_SERVICE = "health_check_service";
    static final String PROTO_REFLECTION_SERVICE = "proto_reflection_service";
    static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    static final String SSL_KEY_FILE = "sslKeyFile";
    static final String ACM_CERT_ARN = "acmCertificateArn";
    static final String ACM_PRIVATE_KEY_PASSWORD = "acmPrivateKeyPassword";
    static final String AWS_REGION = "awsRegion";
    static final String THREAD_COUNT = "thread_count";
    static final String MAX_CONNECTION_COUNT = "max_connection_count";
    static final String ENABLE_UNFRAMED_REQUESTS = "unframed_requests";
    static final String COMPRESSION = "compression";
    static final String RETRY_INFO = "retry_info";
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final int DEFAULT_PORT = 21891;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final boolean DEFAULT_SSL = true;
    static final boolean DEFAULT_ENABLED_UNFRAMED_REQUESTS = false;
    static final boolean DEFAULT_HEALTH_CHECK = false;
    static final boolean DEFAULT_PROTO_REFLECTION_SERVICE = false;
    static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;
    private static final String S3_PREFIX = "s3://";
    static final String UNAUTHENTICATED_HEALTH_CHECK = "unauthenticated_health_check";

    @JsonProperty(REQUEST_TIMEOUT)
    private int requestTimeoutInMillis = DEFAULT_REQUEST_TIMEOUT_MS;

    @JsonProperty(PORT)
    private int port = DEFAULT_PORT;

    @JsonProperty(PATH)
    @Size(min = 1, message = "path length should be at least 1")
    private String path;

    @JsonProperty(HEALTH_CHECK_SERVICE)
    private boolean healthCheck = DEFAULT_HEALTH_CHECK;

    @JsonProperty(PROTO_REFLECTION_SERVICE)
    private boolean protoReflectionService = DEFAULT_PROTO_REFLECTION_SERVICE;

    @JsonProperty(ENABLE_UNFRAMED_REQUESTS)
    private boolean enableUnframedRequests = DEFAULT_ENABLED_UNFRAMED_REQUESTS;

    @JsonProperty(SSL)
    private boolean ssl = DEFAULT_SSL;

    @JsonProperty("output_format")
    private OTelOutputFormat outputFormat = OTelOutputFormat.OPENSEARCH;

    @JsonProperty(USE_ACM_CERT_FOR_SSL)
    private boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

    @JsonProperty(ACM_CERT_ISSUE_TIME_OUT_MILLIS)
    private long acmCertIssueTimeOutMillis = DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS;

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

    @AssertTrue(message = "path should start with /")
    boolean isPathValid() {
        return path == null || path.startsWith("/");
    }

    public void validateAndInitializeCertAndKeyFileInS3() {
        boolean certAndKeyFileInS3 = false;
        if (useAcmCertForSSL) {
            validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), acmCertificateArn, ACM_CERT_ARN);
            validateSSLArgument(String.format("%s is enabled", USE_ACM_CERT_FOR_SSL), awsRegion, AWS_REGION);
        } else if(ssl) {
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
            throw new IllegalArgumentException(String.format("%s, %s can not be empty or null", sslTypeMessage, argumentName));
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

    public int getRequestTimeoutInMillis() {
        return requestTimeoutInMillis;
    }

    public int getPort() {
        return port;
    }

    public String getPath() {
        return path;
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

    public OTelOutputFormat getOutputFormat() {
        return outputFormat;
    }

    public boolean useAcmCertForSSL() {
        return useAcmCertForSSL;
    }

    public long getAcmCertIssueTimeOutMillis() {
        return acmCertIssueTimeOutMillis;
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

    public PluginModel getAuthentication() { return authentication; }

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

    public void setRetryInfo(RetryInfoConfig retryInfo) {
        this.retryInfo = retryInfo;
    }
}

