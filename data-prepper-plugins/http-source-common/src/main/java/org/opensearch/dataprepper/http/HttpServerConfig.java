/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.core.instrument.util.StringUtils;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;

/**
 * HttpServerConfig class holds the common http configurations defined in http source.
*/
public class HttpServerConfig {
    static final String SSL = "ssl";
    static final String SSL_CERTIFICATE_FILE = "ssl_certificate_file";
    static final String SSL_KEY_FILE = "ssl_key_file";
    static final boolean DEFAULT_USE_ACM_CERTIFICATE_FOR_SSL = false;
    static final int DEFAULT_ACM_CERTIFICATE_TIMEOUT_MILLIS = 120000;
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final double BUFFER_TIMEOUT_FRACTION = 0.8;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final int DEFAULT_MAX_PENDING_REQUESTS = 1024;
    static final boolean DEFAULT_HEALTH_CHECK = false;
    static final String HEALTH_CHECK_SERVICE = "health_check_service";
    static final String UNAUTHENTICATED_HEALTH_CHECK = "unauthenticated_health_check";
    static final String S3_PREFIX = "s3://";

    @JsonProperty("request_timeout")
    @Min(2)
    private int requestTimeoutInMillis = DEFAULT_REQUEST_TIMEOUT_MS;

    @JsonProperty("thread_count")
    @Min(0)
    private int threadCount = DEFAULT_THREAD_COUNT;

    @JsonProperty("max_connection_count")
    @Min(0)
    private int maxConnectionCount = DEFAULT_MAX_CONNECTION_COUNT;

    @JsonProperty("max_pending_requests")
    @Min(0)
    private int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;

    @JsonProperty(SSL)
    private boolean ssl;

    @JsonProperty(SSL_CERTIFICATE_FILE)
    private String sslCertificateFile;

    @JsonProperty(SSL_KEY_FILE)
    private String sslKeyFile;

    @JsonProperty("use_acm_certificate_for_ssl")
    private boolean useAcmCertificateForSsl = DEFAULT_USE_ACM_CERTIFICATE_FOR_SSL;

    @JsonProperty("acm_certificate_arn")
    private String acmCertificateArn;

    @JsonProperty("acm_private_key_password")
    private String acmPrivateKeyPassword;

    @JsonProperty("acm_certificate_timeout_millis")
    @Min(0)
    private Integer acmCertificateTimeoutMillis = DEFAULT_ACM_CERTIFICATE_TIMEOUT_MILLIS;

    @JsonProperty("aws_region")
    private String awsRegion;

    @JsonProperty(HEALTH_CHECK_SERVICE)
    private boolean healthCheckService = DEFAULT_HEALTH_CHECK;

    @JsonProperty(UNAUTHENTICATED_HEALTH_CHECK)
    private boolean unauthenticatedHealthCheck = false;

    @JsonProperty("max_request_length")
    private ByteCount maxRequestLength;

    private PluginModel authentication;

    public boolean isSslCertAndKeyFileInS3() {
        return ssl && sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                sslKeyFile.toLowerCase().startsWith(S3_PREFIX);
    }

    @AssertTrue(message = "ssl_certificate_file cannot be a empty or null when ssl is enabled")
    boolean isSslCertificateFileValid() {
        if (ssl && !useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(sslCertificateFile);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "ssl_key_file cannot be a empty or null when ssl is enabled")
    boolean isSslKeyFileValid() {
        if (ssl && !useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(sslKeyFile);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "acm_certificate_arn cannot be a empty or null when ACM is used for ssl")
    boolean isAcmCertificateArnValid() {
        if (ssl && useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(acmCertificateArn);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "aws_region cannot be a empty or null when ACM / S3 is used for ssl")
    boolean isAwsRegionValid() {
        if (ssl && (useAcmCertificateForSsl || isSslCertAndKeyFileInS3())) {
            return StringUtils.isNotEmpty(awsRegion);
        }
        return true;
    }

    public int getRequestTimeoutInMillis() {
        return requestTimeoutInMillis;
    }

    public int getBufferTimeoutInMillis() {
        return (int)(BUFFER_TIMEOUT_FRACTION * requestTimeoutInMillis);
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getMaxConnectionCount() {
        return maxConnectionCount;
    }

    public int getMaxPendingRequests() {
        return maxPendingRequests;
    }

    public boolean isSsl() {
        return ssl;
    }

    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public boolean isUseAcmCertificateForSsl() {
        return useAcmCertificateForSsl;
    }

    public String getAcmCertificateArn() {
        return acmCertificateArn;
    }

    public String getAcmPrivateKeyPassword() {
        return acmPrivateKeyPassword;
    }

    public int getAcmCertificateTimeoutMillis() {
        return acmCertificateTimeoutMillis;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }

    public boolean hasHealthCheckService() {
        return healthCheckService;
    }

    public boolean isUnauthenticatedHealthCheck() {
        return unauthenticatedHealthCheck;
    }

    public ByteCount getMaxRequestLength() {
        return maxRequestLength;
    }
}
