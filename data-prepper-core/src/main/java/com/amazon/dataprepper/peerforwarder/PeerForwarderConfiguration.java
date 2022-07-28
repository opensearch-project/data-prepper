/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    private boolean ssl = true;
    private String sslCertificateFile;
    private String sslKeyFile;
    private DiscoveryMode discoveryMode = DiscoveryMode.STATIC;
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
        setSslCertificateFile(sslCertificateFile);
        setSslKeyFile(sslKeyFile);
        setBatchSize(batchSize);
        setBufferSize(bufferSize);
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
        if (serverPort != null) {
            if (serverPort < 0 || serverPort > 65535) {
                throw new IllegalArgumentException("Server port should be between 0 and 65535");
            }
            this.serverPort = serverPort;
        }
    }

    private void setRequestTimeout(final Integer requestTimeout) {
        if (requestTimeout!= null) {
            if (requestTimeout <= 0) {
                throw new IllegalArgumentException("Request timeout must be a positive integer");
            }
            this.requestTimeout = requestTimeout;
        }
    }

    private void setThreadCount(final Integer threadCount) {
        if (threadCount != null) {
            if (threadCount <= 0) {
                throw new IllegalArgumentException("Thread count must be a positive integer");
            }
            this.threadCount = threadCount;
        }
    }

    private void setMaxConnectionCount(final Integer maxConnectionCount) {
        if (maxConnectionCount != null) {
            if (maxConnectionCount <= 0) {
                throw new IllegalArgumentException("Maximum connection count must be a positive integer");
            }
            this.maxConnectionCount = maxConnectionCount;
        }
    }

    private void setMaxPendingRequests(final Integer maxPendingRequests) {
        if (maxPendingRequests != null) {
            if (maxPendingRequests <= 0) {
                throw new IllegalArgumentException("Maximum pending requests must be a positive integer");
            }
            this.maxPendingRequests = maxPendingRequests;
        }
    }

    private void setSsl(final Boolean ssl) {
        if (ssl != null) {
            this.ssl = ssl;
        }
    }

    private void setSslCertificateFile(String sslCertificateFile) {
        if (!ssl || isValidFilePath(sslCertificateFile)) {
            this.sslCertificateFile = sslCertificateFile;
        }
        else {
            throw new IllegalArgumentException("SSL certificate file path must be a valid file path when ssl is enabled.");
        }
    }

    private void setSslKeyFile(String sslKeyFile) {
        if (!ssl || isValidFilePath(sslKeyFile)) {
            this.sslKeyFile = sslKeyFile;
        }
        else {
            throw new IllegalArgumentException("SSL key file path must be a valid file path when ssl is enabled.");
        }
    }

    private void setDiscoveryMode(final String discoveryMode) {
        if (discoveryMode != null) {
            this.discoveryMode = DiscoveryMode.valueOf(discoveryMode.toUpperCase());
        }
    }

    private void setBatchSize(final Integer batchSize) {
        if (batchSize != null) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("Batch size must be a positive integer");
            }
            this.batchSize = batchSize;
        }
    }

    private void setBufferSize(final Integer bufferSize) {
        if (bufferSize != null) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be a positive integer");
            }
            this.bufferSize = bufferSize;
        }
    }

    private static boolean isValidFilePath(final String filePath) {
        return filePath != null && !filePath.isEmpty() && Files.exists(Paths.get(filePath));
    }
}
