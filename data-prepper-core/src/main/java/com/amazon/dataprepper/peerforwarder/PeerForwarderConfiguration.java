/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class to hold configuration for Core Peer Forwarder in {@link com.amazon.dataprepper.parser.model.DataPrepperConfiguration},
 * including port, ssl, buffer, peer forwarder client and server configuration.
 * @since 2.0
 */
public class PeerForwarderConfiguration {
    private static final String S3_PREFIX = "s3://";

    private Integer serverPort = 21890;
    private Integer requestTimeout = 10_000;
    private Integer threadCount = 200;
    private Integer maxConnectionCount = 500;
    private Integer maxPendingRequests = 1024;
    private boolean ssl = true;
    private String sslCertificateFile;
    private String sslKeyFile;
    private boolean useAcmCertificateForSsl = false;
    private String acmCertificateArn;
    private String acmPrivateKeyPassword;
    private Integer acmCertificateTimeoutMillis = 120000;
    private DiscoveryMode discoveryMode = DiscoveryMode.STATIC;
    private String awsCloudMapNamespaceName;
    private String awsCloudMapServiceName;
    private String awsRegion;
    private Map<String, String> awsCloudMapQueryParameters = Collections.emptyMap();
    private String domainName;
    private List<String> staticEndpoints = new ArrayList<>();
    private Integer batchSize = 48;
    private Integer bufferSize = 512;
    private boolean sslCertAndKeyFileInS3;

    public PeerForwarderConfiguration() {}

    @JsonCreator
    public PeerForwarderConfiguration (
            @JsonProperty("port") final Integer serverPort,
            @JsonProperty("request_timeout") final Integer requestTimeout,
            @JsonProperty("thread_count") final Integer threadCount,
            @JsonProperty("max_connection_count") final Integer maxConnectionCount,
            @JsonProperty("max_pending_requests") final Integer maxPendingRequests,
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("ssl_certificate_file") final String sslCertificateFile,
            @JsonProperty("ssl_key_file") final String sslKeyFile,
            @JsonProperty("use_acm_certificate_for_ssl") final Boolean useAcmCertificateForSsl,
            @JsonProperty("acm_certificate_arn") final String acmCertificateArn,
            @JsonProperty("acm_private_key_password") final String acmPrivateKeyPassword,
            @JsonProperty("acm_certificate_timeout_millis") final Integer acmCertificateTimeoutMillis,
            @JsonProperty("discovery_mode") final String discoveryMode,
            @JsonProperty("aws_cloud_map_namespace_name") final String awsCloudMapNamespaceName,
            @JsonProperty("aws_cloud_map_service_name") final String awsCloudMapServiceName,
            @JsonProperty("aws_region") final String awsRegion,
            @JsonProperty("aws_cloud_map_query_parameters") final Map<String, String> awsCloudMapQueryParameters,
            @JsonProperty("domain_name") final String domainName,
            @JsonProperty("static_endpoints") final List<String> staticEndpoints,
            @JsonProperty("batch_size") final Integer batchSize,
            @JsonProperty("buffer_size") final Integer bufferSize
    ) {
        setServerPort(serverPort);
        setRequestTimeout(requestTimeout);
        setThreadCount(threadCount);
        setMaxConnectionCount(maxConnectionCount);
        setMaxPendingRequests(maxPendingRequests);
        setSsl(ssl);
        setSslCertificateFile(sslCertificateFile);
        setSslKeyFile(sslKeyFile);
        setUseAcmCertificateForSsl(useAcmCertificateForSsl);
        setAcmCertificateArn(acmCertificateArn);
        this.acmPrivateKeyPassword = acmPrivateKeyPassword;
        setAcmCertificateTimeoutMillis(acmCertificateTimeoutMillis);
        setDiscoveryMode(discoveryMode);
        setAwsCloudMapNamespaceName(awsCloudMapNamespaceName);
        setAwsCloudMapServiceName(awsCloudMapServiceName);
        setAwsRegion(awsRegion);
        setAwsCloudMapQueryParameters(awsCloudMapQueryParameters);
        setDomainName(domainName);
        setStaticEndpoints(staticEndpoints);
        setBatchSize(batchSize);
        setBufferSize(bufferSize);
        checkForCertAndKeyFileInS3();
    }

    public int getServerPort() {
        return serverPort;
    }

    public int getRequestTimeout() {
        return requestTimeout;
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

    public DiscoveryMode getDiscoveryMode() {
        return discoveryMode;
    }

    public String getAwsCloudMapNamespaceName() {
        return awsCloudMapNamespaceName;
    }

    public String getAwsCloudMapServiceName() {
        return awsCloudMapServiceName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public Map<String, String> getAwsCloudMapQueryParameters() {
        return awsCloudMapQueryParameters;
    }

    public List<String> getStaticEndpoints() {
        return staticEndpoints;
    }

    public String getDomainName() {
        return domainName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    private void setServerPort(final Integer serverPort) {
        if (serverPort != null) {
            if (serverPort < 0 || serverPort > 65535) {
                throw new IllegalArgumentException("Server port should be between 0 and 65535.");
            }
            this.serverPort = serverPort;
        }
    }

    private void setRequestTimeout(final Integer requestTimeout) {
        if (requestTimeout!= null) {
            if (requestTimeout <= 0) {
                throw new IllegalArgumentException("Request timeout must be a positive integer.");
            }
            this.requestTimeout = requestTimeout;
        }
    }

    private void setThreadCount(final Integer threadCount) {
        if (threadCount != null) {
            if (threadCount <= 0) {
                throw new IllegalArgumentException("Thread count must be a positive integer.");
            }
            this.threadCount = threadCount;
        }
    }

    private void setMaxConnectionCount(final Integer maxConnectionCount) {
        if (maxConnectionCount != null) {
            if (maxConnectionCount <= 0) {
                throw new IllegalArgumentException("Maximum connection count must be a positive integer.");
            }
            this.maxConnectionCount = maxConnectionCount;
        }
    }

    private void setMaxPendingRequests(final Integer maxPendingRequests) {
        if (maxPendingRequests != null) {
            if (maxPendingRequests <= 0) {
                throw new IllegalArgumentException("Maximum pending requests must be a positive integer.");
            }
            this.maxPendingRequests = maxPendingRequests;
        }
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    private void setSslCertificateFile(final String sslCertificateFile) {
        if (!ssl || StringUtils.isNotEmpty(sslCertificateFile)) {
            this.sslCertificateFile = sslCertificateFile;
        }
        else {
            throw new IllegalArgumentException("SSL certificate file path must be a valid file path when ssl is enabled.");
        }
    }

    private void setSslKeyFile(final String sslKeyFile) {
        if (!ssl || StringUtils.isNotEmpty(sslKeyFile)) {
            this.sslKeyFile = sslKeyFile;
        }
        else {
            throw new IllegalArgumentException("SSL key file path must be a valid file path when ssl is enabled.");
        }
    }

    private void setUseAcmCertificateForSsl(final Boolean useAcmCertificateForSsl) {
        if (useAcmCertificateForSsl != null) {
            this.useAcmCertificateForSsl = useAcmCertificateForSsl;
        }
    }

    private void setAcmCertificateArn(final String acmCertificateArn) {
        if (!useAcmCertificateForSsl || StringUtils.isNotEmpty(acmCertificateArn)) {
            this.acmCertificateArn = acmCertificateArn;
        }
        else {
            throw new IllegalArgumentException("ACM certificate ARN cannot be empty if ACM certificate is ued for SSL.");
        }
    }

    private void setAcmCertificateTimeoutMillis(final Integer acmCertificateTimeoutMillis) {
        if (acmCertificateTimeoutMillis != null) {
            if (acmCertificateTimeoutMillis <= 0) {
                throw new IllegalArgumentException("ACM certificate timeout must be a positive integer");
            }
            this.acmCertificateTimeoutMillis = acmCertificateTimeoutMillis;
        }
    }

    private void setDiscoveryMode(final String discoveryMode) {
        if (discoveryMode != null) {
            this.discoveryMode = DiscoveryMode.valueOf(discoveryMode.toUpperCase());
        }
    }

    private void setAwsCloudMapNamespaceName(final String awsCloudMapNamespaceName) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP)) {
            if (awsCloudMapNamespaceName != null) {
                this.awsCloudMapNamespaceName = awsCloudMapNamespaceName;
            }
            else {
                throw new IllegalArgumentException("Cloud Map namespace cannot be null if discover mode is AWS Cloud Map.");
            }
        }
    }

    private void setAwsCloudMapServiceName(final String awsCloudMapServiceName) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP)) {
            if (awsCloudMapServiceName != null) {
                this.awsCloudMapServiceName = awsCloudMapServiceName;
            }
            else {
                throw new IllegalArgumentException("Cloud Map service name cannot be null if discover mode is AWS Cloud Map.");
            }
        }
    }

    private void setAwsRegion(final String awsRegion) {
        if (discoveryMode.equals(DiscoveryMode.AWS_CLOUD_MAP) || useAcmCertificateForSsl) {
            if (StringUtils.isNotEmpty(awsRegion)) {
                this.awsRegion = awsRegion;
            }
            else {
                throw new IllegalArgumentException("AWS region cannot be null if discover mode is AWS Cloud Map or if ACM certificate for SLL is enabled.");
            }
        }
    }

    private void setAwsCloudMapQueryParameters(Map<String, String> awsCloudMapQueryParameters) {
        if (awsCloudMapQueryParameters != null) {
            this.awsCloudMapQueryParameters = awsCloudMapQueryParameters;
        }
    }

    private void setDomainName(final String domainName) {
        if (discoveryMode.equals(DiscoveryMode.DNS)) {
            if (domainName != null) {
                this.domainName = domainName;
            }
            else {
                throw new IllegalArgumentException("Domain name cannot be null if discover mode is DNS.");
            }
        }
    }

    private void setStaticEndpoints(final List<String> staticEndpoints) {
        if (staticEndpoints != null) {
            this.staticEndpoints = staticEndpoints;
        }
    }

    private void setBatchSize(final Integer batchSize) {
        if (batchSize != null) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be a positive integer.");
            }
            this.batchSize = batchSize;
        }
    }

    private void setBufferSize(final Integer bufferSize) {
        if (bufferSize != null) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be a positive integer.");
            }
            this.bufferSize = bufferSize;
        }
    }

    private void checkForCertAndKeyFileInS3() {
        if (ssl && sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                    sslKeyFile.toLowerCase().startsWith(S3_PREFIX)) {
            sslCertAndKeyFileInS3 = true;
        }
    }

    public boolean isSslCertAndKeyFileInS3() {
        return sslCertAndKeyFileInS3;
    }
}
