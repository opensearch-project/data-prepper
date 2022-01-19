/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.nio.file.Files;
import java.nio.file.Paths;

public class HTTPSourceConfig {
    static final String DEFAULT_LOG_INGEST_URI = "/log/ingest";
    static final String SSL = "ssl";
    static final String SSL_CERTIFICATE_FILE = "ssl_certificate_file";
    static final String SSL_KEY_FILE = "ssl_key_file";
    static final int DEFAULT_PORT = 2021;
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final int DEFAULT_MAX_PENDING_REQUESTS = 1024;

    @JsonProperty("port")
    @Min(0)
    @Max(65535)
    private int port = DEFAULT_PORT;

    @JsonProperty("request_timeout")
    @Min(0)
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

    @JsonProperty("ssl_key_password")
    private String sslKeyPassword;

    private PluginModel authentication;

    @AssertTrue(message = "ssl_certificate_file must be a valid file path when ssl is enabled")
    boolean isSslCertificateFileValidation() {
        return !ssl || isValidFilePath(sslCertificateFile);
    }

    @AssertTrue(message = "ssl_key_file must be a valid file path when ssl is enabled")
    boolean isSslKeyFileValidation() {
        return !ssl || isValidFilePath(sslKeyFile);
    }

    private static boolean isValidFilePath(final String filePath) {
        return filePath != null && !filePath.isEmpty() && Files.exists(Paths.get(filePath));
    }


    public int getPort() {
        return port;
    }

    public int getRequestTimeoutInMillis() {
        return requestTimeoutInMillis;
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

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public PluginModel getAuthentication() {
        return authentication;
    }
}
