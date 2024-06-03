package org.opensearch.dataprepper.http;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.core.instrument.util.StringUtils;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

/**
 * BaseHttpServerConfig class holds the common Http related configurations defined in the customer's source configuration along with default set of configuration values.
*/
public class BaseHttpServerConfig implements HttpServerConfig {
    static final String COMPRESSION = "compression";
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

    @JsonProperty("port")
    @Min(0)
    @Max(65535)
    private int port = getDefaultPort();

    @Override
    public int getDefaultPort() {
        return 0;
    }

    @JsonProperty("path")
    @Size(min = 1, message = "path length should be at least 1")
    private String path = getDefaultPath();

    @Override
    public String getDefaultPath() {
        return "";
    }

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

    @JsonProperty(COMPRESSION)
    private CompressionOption compression = CompressionOption.NONE;

    @Override
    @AssertTrue(message = "path should start with /")
    public boolean isPathValid() {
        return path.startsWith("/");
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public CompressionOption getCompression() {
        return compression;
    }

    @Override
    public boolean isSslCertAndKeyFileInS3() {
        return ssl && sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                sslKeyFile.toLowerCase().startsWith(S3_PREFIX);
    }

    @AssertTrue(message = "ssl_certificate_file cannot be a empty or null when ssl is enabled")
    @Override
    public boolean isSslCertificateFileValid() {
        if (ssl && !useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(sslCertificateFile);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "ssl_key_file cannot be a empty or null when ssl is enabled")
    @Override
    public boolean isSslKeyFileValid() {
        if (ssl && !useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(sslKeyFile);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "acm_certificate_arn cannot be a empty or null when ACM is used for ssl")
    @Override
    public boolean isAcmCertificateArnValid() {
        if (ssl && useAcmCertificateForSsl) {
            return StringUtils.isNotEmpty(acmCertificateArn);
        }
        else {
            return true;
        }
    }

    @AssertTrue(message = "aws_region cannot be a empty or null when ACM / S3 is used for ssl")
    @Override
    public boolean isAwsRegionValid() {
        if (ssl && (useAcmCertificateForSsl || isSslCertAndKeyFileInS3())) {
            return StringUtils.isNotEmpty(awsRegion);
        }
        return true;
    }

    @Override
    public int getRequestTimeoutInMillis() {
        return requestTimeoutInMillis;
    }

    @Override
    public int getBufferTimeoutInMillis() {
        return (int)(BUFFER_TIMEOUT_FRACTION * requestTimeoutInMillis);
    }

    @Override
    public int getThreadCount() {
        return threadCount;
    }

    @Override
    public int getMaxConnectionCount() {
        return maxConnectionCount;
    }

    @Override
    public int getMaxPendingRequests() {
        return maxPendingRequests;
    }

    @Override
    public boolean isSsl() {
        return ssl;
    }

    @Override
    public String getSslCertificateFile() {
        return sslCertificateFile;
    }

    @Override
    public String getSslKeyFile() {
        return sslKeyFile;
    }

    @Override
    public boolean isUseAcmCertificateForSsl() {
        return useAcmCertificateForSsl;
    }

    @Override
    public String getAcmCertificateArn() {
        return acmCertificateArn;
    }

    @Override
    public String getAcmPrivateKeyPassword() {
        return acmPrivateKeyPassword;
    }

    @Override
    public int getAcmCertificateTimeoutMillis() {
        return acmCertificateTimeoutMillis;
    }

    @Override
    public String getAwsRegion() {
        return awsRegion;
    }

    @Override
    public PluginModel getAuthentication() {
        return authentication;
    }

    @Override
    public boolean hasHealthCheckService() {
        return healthCheckService;
    }

    @Override
    public boolean isUnauthenticatedHealthCheck() {
        return unauthenticatedHealthCheck;
    }

    @Override
    public ByteCount getMaxRequestLength() {
        return maxRequestLength;
    }
}
