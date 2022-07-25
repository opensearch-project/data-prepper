/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Class to hold configuration for Core Peer Forwarder in {@link com.amazon.dataprepper.parser.model.DataPrepperConfiguration},
 * including port, ssl, buffer, peer forwarder client and server configuration.
 * @since 2.0
 */

public class PeerForwarderConfiguration {
    private Integer serverPort = 21890;
    private Integer requestTimeout = 10_000;
    private Integer threadCount = 200;
    private Integer maxConnectionCount = 500;
    private Integer maxPendingRequests = 1024;
    private Boolean ssl = true;
    private String sslCertificateFile;
    private String sslKeyFile;
    private DiscoveryMode discoveryMode;
    private Integer batchSize = 48;
    private Integer bufferSize = 512;

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
            @JsonProperty("discovery_mode") final String discoveryMode,
            @JsonProperty("batch_size") final Integer batchSize,
            @JsonProperty("buffer_size") final Integer bufferSize
    ) {
        setServerPort(serverPort);
        setRequestTimeout(requestTimeout);
        setThreadCount(threadCount);
        setMaxConnectionCount(maxConnectionCount);
        setMaxPendingRequests(maxPendingRequests);
        setSsl(ssl);
        setDiscoveryMode(discoveryMode);
        this.sslCertificateFile = sslCertificateFile != null ? sslCertificateFile : "";
        this.sslKeyFile = sslKeyFile != null ? sslKeyFile : "";
        setBatchSize(batchSize);
        setBufferSize(bufferSize);
    }

    @AssertTrue(message = "ssl_certificate_file must be a valid file path when ssl is enabled")
    boolean isSslCertificateFileValid() {
        return !ssl || isValidFilePath(sslCertificateFile);
    }

    @AssertTrue(message = "ssl_key_file must be a valid file path when ssl is enabled")
    boolean isSslKeyFileValid() {
        return !ssl || isValidFilePath(sslKeyFile);
    }

    private static boolean isValidFilePath(final String filePath) {
        return filePath != null && !filePath.isEmpty() && Files.exists(Paths.get(filePath));
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

    public DiscoveryMode getDiscoveryMode() {
        return discoveryMode;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    private void setServerPort(final Integer serverPort) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Server port must be a positive integer");
        }
        this.serverPort = serverPort;
    }

    private void setRequestTimeout(final Integer requestTimeout) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Request timeout must be a positive integer");
        }
        this.requestTimeout = requestTimeout;
    }

    private void setThreadCount(final Integer threadCount) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Thread count must be a positive integer");
        }
        this.threadCount = threadCount;
    }

    private void setMaxConnectionCount(final Integer maxConnectionCount) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Maximum connection count must be a positive integer");
        }
        this.maxConnectionCount = maxConnectionCount;
    }

    private void setMaxPendingRequests(final Integer maxPendingRequests) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Maximum pending requests must be a positive integer");
        }
        this.maxPendingRequests = maxPendingRequests;
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    public void setDiscoveryMode(final String discoveryMode) {
        if (discoveryMode != null)
            this.discoveryMode = DiscoveryMode.valueOf(discoveryMode.toUpperCase());
    }

    private void setBatchSize(final Integer batchSize) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Batch size must be a positive integer");
        }
        this.batchSize = batchSize;
    }

    private void setBufferSize(final Integer bufferSize) {
        if (serverPort <= 0) {
            throw new IllegalArgumentException("Buffer size must be a positive integer");
        }
        this.bufferSize = bufferSize;
    }
}
