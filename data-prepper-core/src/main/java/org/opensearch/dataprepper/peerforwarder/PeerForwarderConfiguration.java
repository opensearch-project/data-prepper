/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.dataprepper.peerforwarder.discovery.DiscoveryMode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class to hold configuration for Core Peer Forwarder in {@link org.opensearch.dataprepper.parser.model.DataPrepperConfiguration},
 * including port, ssl, buffer, peer forwarder client and server configuration.
 * @since 2.0
 */
public class PeerForwarderConfiguration {
    public static final String DEFAULT_PEER_FORWARDING_URI = "/event/forward";
    public static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(10L);
    public static final Duration DEFAULT_FORWARDING_BATCH_TIMEOUT = Duration.ofSeconds(3L);
    public static final String DEFAULT_CERTIFICATE_FILE_PATH = "config/default_certificate.pem";
    public static final String DEFAULT_PRIVATE_KEY_FILE_PATH = "config/default_private_key.pem";
    private static final String S3_PREFIX = "s3://";
    public static final int MAX_FORWARDING_BATCH_SIZE = 15000;

    private Integer serverPort = 4994;
    private Integer requestTimeout = 10_000;
    private Integer clientTimeout = 60_000;
    private Integer serverThreadCount = 200;
    private Integer maxConnectionCount = 500;
    private Integer maxPendingRequests = 1024;
    private boolean ssl = true;
    private String sslCertificateFile = DEFAULT_CERTIFICATE_FILE_PATH;
    private String sslKeyFile = DEFAULT_PRIVATE_KEY_FILE_PATH;
    private boolean sslDisableVerification = false;
    private boolean sslFingerprintVerificationOnly = false;
    private ForwardingAuthentication authentication = ForwardingAuthentication.UNAUTHENTICATED;
    private boolean useAcmCertificateForSsl = false;
    private String acmCertificateArn;
    private String acmPrivateKeyPassword;
    private Integer acmCertificateTimeoutMillis = 120000;
    private DiscoveryMode discoveryMode = DiscoveryMode.LOCAL_NODE;
    private String awsCloudMapNamespaceName;
    private String awsCloudMapServiceName;
    private String awsRegion;
    private Map<String, String> awsCloudMapQueryParameters = Collections.emptyMap();
    private String domainName;
    private List<String> staticEndpoints = new ArrayList<>();
    private Integer clientThreadCount = 200;
    private Integer batchSize = 48;
    private Integer batchDelay = 3_000;
    private Integer bufferSize = 512;
    private boolean sslCertAndKeyFileInS3 = false;
    private Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;
    private Integer failedForwardingRequestLocalWriteTimeout = 500;
    private Integer forwardingBatchSize = 1500;
    private Integer forwardingBatchQueueDepth = 1;
    private Duration forwardingBatchTimeout = DEFAULT_FORWARDING_BATCH_TIMEOUT;
    private boolean binaryCodec = true;

    public PeerForwarderConfiguration() {}

    @JsonCreator
    public PeerForwarderConfiguration (
            @JsonProperty("port") final Integer serverPort,
            @JsonProperty("request_timeout") final Integer requestTimeout,
            @JsonProperty("client_timeout") final Integer clientTimeout,
            @JsonProperty("server_thread_count") final Integer serverThreadCount,
            @JsonProperty("max_connection_count") final Integer maxConnectionCount,
            @JsonProperty("max_pending_requests") final Integer maxPendingRequests,
            @JsonProperty("ssl") final Boolean ssl,
            @JsonProperty("ssl_certificate_file") final String sslCertificateFile,
            @JsonProperty("ssl_key_file") final String sslKeyFile,
            @JsonProperty("ssl_insecure_disable_verification") final boolean sslDisableVerification,
            @JsonProperty("ssl_fingerprint_verification_only") final boolean sslFingerprintVerificationOnly,
            @JsonProperty("authentication") final Map<String, Object> authentication,
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
            @JsonProperty("client_thread_count") final Integer clientThreadCount,
            @JsonProperty("batch_size") final Integer batchSize,
            @JsonProperty("batch_delay") final Integer batchDelay,
            @JsonProperty("buffer_size") final Integer bufferSize,
            @JsonProperty("drain_timeout") final Duration drainTimeout,
            @JsonProperty("failed_forwarding_requests_local_write_timeout") final Integer failedForwardingRequestLocalWriteTimeout,
            @JsonProperty("forwarding_batch_size") final Integer forwardingBatchSize,
            @JsonProperty("forwarding_batch_queue_depth") final Integer forwardingBatchQueueDepth,
            @JsonProperty("forwarding_batch_timeout") final Duration forwardingBatchTimeout,
            @JsonProperty("binary_codec") final Boolean binaryCodec
    ) {
        setServerPort(serverPort);
        setRequestTimeout(requestTimeout);
        setClientTimeout(clientTimeout);
        setServerThreadCount(serverThreadCount);
        setMaxConnectionCount(maxConnectionCount);
        setMaxPendingRequests(maxPendingRequests);
        setSsl(ssl);
        setUseAcmCertificateForSsl(useAcmCertificateForSsl);
        setSslCertificateFile(sslCertificateFile);
        setSslKeyFile(sslKeyFile);
        setDisableVerification(sslDisableVerification);
        setFingerprintVerificationOnly(sslFingerprintVerificationOnly);
        setAuthentication(authentication);
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
        setClientThreadCount(clientThreadCount);
        setBatchSize(batchSize);
        setBatchDelay(batchDelay);
        setBufferSize(bufferSize);
        setDrainTimeout(drainTimeout);
        setFailedForwardingRequestLocalWriteTimeout(failedForwardingRequestLocalWriteTimeout);
        setForwardingBatchSize(forwardingBatchSize);
        setForwardingBatchQueueDepth(forwardingBatchQueueDepth);
        setForwardingBatchTimeout(forwardingBatchTimeout);
        setBinaryCodec(binaryCodec == null || binaryCodec);
        checkForCertAndKeyFileInS3();
        validateSslAndAuthentication();
    }

    public int getServerPort() {
        return serverPort;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getClientTimeout() {
        return clientTimeout;
    }

    public int getServerThreadCount() {
        return serverThreadCount;
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

    public Integer getClientThreadCount() {
        return clientThreadCount;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Integer getBatchDelay() {
        return batchDelay;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public Duration getDrainTimeout() {
        return drainTimeout;
    }

    public Integer getFailedForwardingRequestLocalWriteTimeout() {
        return failedForwardingRequestLocalWriteTimeout;
    }

    public Integer getForwardingBatchSize() {
        return forwardingBatchSize;
    }

    public Integer getForwardingBatchQueueDepth() {
        return forwardingBatchQueueDepth;
    }

    public Duration getForwardingBatchTimeout() {
        return forwardingBatchTimeout;
    }

    public boolean getBinaryCodec() {
        return binaryCodec;
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
            if (requestTimeout <= 1) {
                throw new IllegalArgumentException("Request timeout must be a positive integer greater than 1.");
            }
            this.requestTimeout = requestTimeout;
        }
    }

    private void setClientTimeout(final Integer clientTimeout) {
        if (clientTimeout != null) {
            if (clientTimeout <= 0) {
                throw new IllegalArgumentException("Client timeout must be a positive integer.");
            }
            this.clientTimeout = clientTimeout;
        }
    }

    private void setServerThreadCount(final Integer serverThreadCount) {
        if (serverThreadCount != null) {
            if (serverThreadCount <= 0) {
                throw new IllegalArgumentException("Server thread count must be a positive integer.");
            }
            this.serverThreadCount = serverThreadCount;
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
        if (ssl && !useAcmCertificateForSsl && sslCertificateFile != null) {
            this.sslCertificateFile = sslCertificateFile;
        }
    }

    private void setSslKeyFile(final String sslKeyFile) {
        if (ssl && !useAcmCertificateForSsl && sslKeyFile != null) {
            this.sslKeyFile = sslKeyFile;
        }
    }

    private void setDisableVerification(final boolean sslDisableVerification) {
        this.sslDisableVerification = sslDisableVerification;
    }

    public boolean isSslDisableVerification() {
        return sslDisableVerification;
    }

    private void setFingerprintVerificationOnly(final boolean sslFingerprintVerificationOnly) {
        this.sslFingerprintVerificationOnly = sslFingerprintVerificationOnly;
    }

    public boolean isSslFingerprintVerificationOnly() {
        return sslFingerprintVerificationOnly;
    }

    private void setAuthentication(final Map<String, Object> authentication) {
        if(authentication == null)
            return;

        if (authentication.isEmpty())
            return;

        if (authentication.size() > 1)
            throw new IllegalArgumentException("Invalid authentication configuration.");

        final String authenticationName = authentication.keySet().iterator().next();

        this.authentication = ForwardingAuthentication.getByName(authenticationName);
    }

    public ForwardingAuthentication getAuthentication() {
        return authentication;
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

    public void setClientThreadCount(final Integer clientThreadCount) {
        if (clientThreadCount != null) {
            if (clientThreadCount <= 0) {
                throw new IllegalArgumentException("Client thread count must be a positive integer.");
            }
            this.clientThreadCount = clientThreadCount;
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

    private void setBatchDelay(final Integer batchDelay) {
        if (batchDelay != null) {
            if (batchDelay < 0) {
                throw new IllegalArgumentException("Batch delay must be a non-negative integer.");
            }
            this.batchDelay = batchDelay;
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
        if (ssl && !useAcmCertificateForSsl && sslCertificateFile.toLowerCase().startsWith(S3_PREFIX) &&
                    sslKeyFile.toLowerCase().startsWith(S3_PREFIX)) {
            sslCertAndKeyFileInS3 = true;
        }
    }

    public boolean isSslCertAndKeyFileInS3() {
        return sslCertAndKeyFileInS3;
    }

    private void validateSslAndAuthentication() {
        if(authentication == ForwardingAuthentication.MUTUAL_TLS && !ssl)
            throw new IllegalArgumentException("Mutual TLS is only available when SSL is enabled.");
    }

    private void setDrainTimeout(final Duration drainTimeout) {
        if (drainTimeout != null) {
            if (drainTimeout.isNegative()) {
                throw new IllegalArgumentException("Peer forwarder drain timeout must be non-negative.");
            }
            this.drainTimeout = drainTimeout;
        }
    }

    private void setFailedForwardingRequestLocalWriteTimeout(final Integer failedForwardingRequestLocalWriteTimeout) {
        if (failedForwardingRequestLocalWriteTimeout != null) {
            if (failedForwardingRequestLocalWriteTimeout <= 0) {
                throw new IllegalArgumentException("Failed forwarding requests local write timeout must be a positive integer.");
            }
            this.failedForwardingRequestLocalWriteTimeout = failedForwardingRequestLocalWriteTimeout;
        }
    }

    private void setForwardingBatchSize(final Integer forwardingBatchSize) {
        if (forwardingBatchSize != null) {
            if (forwardingBatchSize <= 0 || forwardingBatchSize > MAX_FORWARDING_BATCH_SIZE) {
                throw new IllegalArgumentException("Forwarding batch size must be between 1 and 3000 inclusive.");
            }
            this.forwardingBatchSize = forwardingBatchSize;
        }
    }

    private void setForwardingBatchQueueDepth(final Integer forwardingBatchQueueDepth) {
        if (forwardingBatchQueueDepth != null) {
            if (forwardingBatchQueueDepth <= 0) {
                throw new IllegalArgumentException("Forwarding batch queue depth must be a positive integer.");
            }
            this.forwardingBatchQueueDepth = forwardingBatchQueueDepth;
        }
    }

    private void setForwardingBatchTimeout(final Duration forwardingBatchTimeout) {
        if (forwardingBatchTimeout != null) {
            if (forwardingBatchTimeout.isNegative()) {
                throw new IllegalArgumentException("Forwarding batch timeout must be non-negative.");
            }
            this.forwardingBatchTimeout = forwardingBatchTimeout;
        }
    }

    private void setBinaryCodec(final boolean binaryCodec) {
        this.binaryCodec = binaryCodec;
    }
}
