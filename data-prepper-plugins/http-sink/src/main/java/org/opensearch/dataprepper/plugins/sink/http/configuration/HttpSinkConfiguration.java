/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.http.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.accumulator.BufferTypeOptions;

import java.time.Duration;
import java.util.List;

public class HttpSinkConfiguration {

    private static final int DEFAULT_UPLOAD_RETRIES = 5;

    private static final int DEFAULT_WORKERS = 1;

    static final boolean DEFAULT_SSL = false;

    private static final String S3_PREFIX = "s3://";

    static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    static final String SSL_KEY_FILE = "sslKeyFile";
    static final String SSL = "ssl";
    static final String AWS_REGION = "awsRegion";
    static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;
    public static final String SSL_IS_ENABLED = "%s is enabled";

    public static final Duration DEFAULT_HTTP_RETRY_INTERVAL = Duration.ofSeconds(30);
    @NotNull
    @JsonProperty("urls")
    private List<UrlConfigurationOption> urlConfigurationOptions;

    @JsonProperty("workers")
    private Integer workers = DEFAULT_WORKERS;

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("http_method")
    private HTTPMethodOptions httpMethod = HTTPMethodOptions.POST;

    @JsonProperty("proxy")
    private String proxy;

    @JsonProperty("auth_type")
    private AuthTypeOptions authType = AuthTypeOptions.UNAUTHENTICATED;

    @JsonProperty("authentication")
    private PluginModel authentication;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    @JsonProperty("ssl_key_file")
    private String sslKeyFile;

    @JsonProperty("aws_sigv4")
    private boolean awsSigv4;

    @JsonProperty("buffer_type")
    private BufferTypeOptions bufferType = BufferTypeOptions.INMEMORY;

    @NotNull
    @JsonProperty("threshold")
    private ThresholdOptions thresholdOptions;

    @JsonProperty("max_retries")
    private int maxUploadRetries = DEFAULT_UPLOAD_RETRIES;

    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @JsonProperty("custom_header")
    private CustomHeaderOptions customHeaderOptions;

    @JsonProperty("dlq_file")
    private String dlqFile;

    @JsonProperty("webhook_url")
    private String webhookURL;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("use_acm_cert_for_ssl")
    private boolean useAcmCertForSSL = DEFAULT_USE_ACM_CERT_FOR_SSL;

    @JsonProperty("acm_private_key_password")
    private String acmPrivateKeyPassword;

    @JsonProperty("acm_certificate_arn")
    private String acmCertificateArn;

    @JsonProperty("acm_cert_issue_time_out_millis")
    private long acmCertIssueTimeOutMillis = DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS;

    @JsonProperty("ssl")
    private boolean ssl = DEFAULT_SSL;

    @JsonProperty("http_retry_interval")
    private Duration httpRetryInterval = DEFAULT_HTTP_RETRY_INTERVAL;


    private boolean sslCertAndKeyFileInS3;

    public boolean isSsl() {
        return ssl;
    }

    public Duration getHttpRetryInterval() {
        return httpRetryInterval;
    }

    public String getAcmPrivateKeyPassword() {
        return acmPrivateKeyPassword;
    }

    public boolean isSslCertAndKeyFileInS3() {
        return sslCertAndKeyFileInS3;
    }

    public long getAcmCertIssueTimeOutMillis() {
        return acmCertIssueTimeOutMillis;
    }

    public boolean useAcmCertForSSL() {
        return useAcmCertForSSL;
    }

    public String getWebhookURL() {
        return webhookURL;
    }

    public void validateAndInitializeCertAndKeyFileInS3() {
        boolean certAndKeyFileInS3 = false;
        if (useAcmCertForSSL) {
            validateSSLArgument(String.format(SSL_IS_ENABLED, useAcmCertForSSL), acmCertificateArn, acmCertificateArn);
            validateSSLArgument(String.format(SSL_IS_ENABLED, useAcmCertForSSL), awsAuthenticationOptions.getAwsRegion().toString(), AWS_REGION);
        } else if(ssl) {
            validateSSLCertificateFiles();
            certAndKeyFileInS3 = isSSLCertificateLocatedInS3();
            if (certAndKeyFileInS3) {
                validateSSLArgument("The certificate and key files are located in S3", awsAuthenticationOptions.getAwsRegion().toString(), AWS_REGION);
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
        validateSSLArgument(String.format(SSL_IS_ENABLED, SSL), sslCertificateFile, SSL_KEY_CERT_FILE);
        validateSSLArgument(String.format(SSL_IS_ENABLED, SSL), sslKeyFile, SSL_KEY_FILE);
    }

    private boolean isSSLCertificateLocatedInS3() {
        return sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                sslKeyFile.toLowerCase().startsWith(S3_PREFIX);
    }

    public String getAcmCertificateArn() {
        return acmCertificateArn;
    }

    public List<UrlConfigurationOption> getUrlConfigurationOptions() {
        return urlConfigurationOptions;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public HTTPMethodOptions getHttpMethod() {
        return httpMethod;
    }

    public String getProxy() {
        return proxy;
    }

    public AuthTypeOptions getAuthType() {
        return authType;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isAwsSigv4() {
        return awsSigv4;
    }

    public BufferTypeOptions getBufferType() {
        return bufferType;
    }

    public ThresholdOptions getThresholdOptions() {
        return thresholdOptions;
    }

    public int getMaxUploadRetries() {
        return maxUploadRetries;
    }

    public CustomHeaderOptions getCustomHeaderOptions() {
        return customHeaderOptions;
    }

    public AwsAuthenticationOptions getAwsAuthenticationOptions() {
        return awsAuthenticationOptions;
    }

    public Integer getWorkers() {
        return workers;
    }

    public String getDlqFile() {
        return dlqFile;
    }

    public PluginModel getDlq() {
        return dlq;
    }
}
