/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import org.apache.commons.lang3.StringUtils;

public class OTelTraceSourceConfig {
    static final String REQUEST_TIMEOUT = "request_timeout";
    static final String PORT = "port";
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
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final int DEFAULT_PORT = 21890;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final boolean DEFAULT_SSL = true;
    static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;
    private static final String S3_PREFIX = "s3://";
    private final int requestTimeoutInMillis;
    private final int port;
    private final boolean healthCheck;
    private final boolean protoReflectionService;
    private final boolean enableUnframedRequests;
    private final boolean ssl;
    private final boolean useAcmCertForSSL;
    private final long acmCertIssueTimeOutMillis;
    private final String sslKeyCertChainFile;
    private final String sslKeyFile;
    private final boolean sslCertAndKeyFileInS3;
    private final String acmCertificateArn;
    private final String acmPrivateKeyPassword;
    private final String awsRegion;
    private final int threadCount;
    private final int maxConnectionCount;

    private OTelTraceSourceConfig(final int requestTimeoutInMillis,
                                  final int port,
                                  final boolean healthCheck,
                                  final boolean protoReflectionService,
                                  final boolean enableUnframedRequests,
                                  final boolean isSSL,
                                  final boolean useAcmCertForSSL,
                                  final long acmCertIssueTimeOutMillis,
                                  final String sslKeyCertChainFile,
                                  final String sslKeyFile,
                                  final String acmCertificateArn,
                                  final String acmPrivateKeyPassword,
                                  final String awsRegion,
                                  final int threadCount,
                                  final int maxConnectionCount) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.port = port;
        this.healthCheck = healthCheck;
        this.protoReflectionService = protoReflectionService;
        this.enableUnframedRequests = enableUnframedRequests;
        this.ssl = isSSL;
        this.useAcmCertForSSL = useAcmCertForSSL;
        this.acmCertIssueTimeOutMillis = acmCertIssueTimeOutMillis;
        this.sslKeyCertChainFile = sslKeyCertChainFile;
        this.sslKeyFile = sslKeyFile;
        this.acmCertificateArn = acmCertificateArn;
        this.acmPrivateKeyPassword = acmPrivateKeyPassword;
        this.awsRegion = awsRegion;
        this.threadCount = threadCount;
        this.maxConnectionCount = maxConnectionCount;
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
        this.sslCertAndKeyFileInS3 = certAndKeyFileInS3;
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

    public static OTelTraceSourceConfig buildConfig(final PluginSetting pluginSetting) {
        return new OTelTraceSourceConfig(pluginSetting.getIntegerOrDefault(REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_MS),
                pluginSetting.getIntegerOrDefault(PORT, DEFAULT_PORT),
                pluginSetting.getBooleanOrDefault(HEALTH_CHECK_SERVICE, false),
                pluginSetting.getBooleanOrDefault(PROTO_REFLECTION_SERVICE, false),
                pluginSetting.getBooleanOrDefault(ENABLE_UNFRAMED_REQUESTS, false),
                pluginSetting.getBooleanOrDefault(SSL, DEFAULT_SSL),
                pluginSetting.getBooleanOrDefault(USE_ACM_CERT_FOR_SSL, DEFAULT_USE_ACM_CERT_FOR_SSL),
                pluginSetting.getLongOrDefault(ACM_CERT_ISSUE_TIME_OUT_MILLIS, DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS),
                pluginSetting.getStringOrDefault(SSL_KEY_CERT_FILE, null),
                pluginSetting.getStringOrDefault(SSL_KEY_FILE, null),
                pluginSetting.getStringOrDefault(ACM_CERT_ARN, null),
                pluginSetting.getStringOrDefault(ACM_PRIVATE_KEY_PASSWORD, null),
                pluginSetting.getStringOrDefault(AWS_REGION, null),
                pluginSetting.getIntegerOrDefault(THREAD_COUNT, DEFAULT_THREAD_COUNT),
                pluginSetting.getIntegerOrDefault(MAX_CONNECTION_COUNT, DEFAULT_MAX_CONNECTION_COUNT));
    }

    public int getRequestTimeoutInMillis() {
        return requestTimeoutInMillis;
    }

    public int getPort() {
        return port;
    }

    public boolean hasHealthCheck() {
        return healthCheck;
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
}
