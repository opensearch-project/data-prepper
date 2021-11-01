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

package com.amazon.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.util.StringUtils;

import java.nio.file.Files;
import java.nio.file.Path;

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
    private int port = DEFAULT_PORT;

    @JsonProperty("request_timeout")
    private int requestTimeoutInMillis = DEFAULT_REQUEST_TIMEOUT_MS;

    @JsonProperty("thread_count")
    private int threadCount = DEFAULT_THREAD_COUNT;

    @JsonProperty("max_connection_count")
    private int maxConnectionCount = DEFAULT_MAX_CONNECTION_COUNT;

    @JsonProperty("max_pending_requests")
    private int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;

    @JsonProperty(SSL)
    private boolean ssl;

    @JsonProperty(SSL_CERTIFICATE_FILE)
    private String sslCertificateFile;

    @JsonProperty(SSL_KEY_FILE)
    private String sslKeyFile;

    @JsonProperty("ssl_key_password")
    private String sslKeyPassword;

    // TODO: Remove once JSR-303 validation is available
    void validate() {
        Preconditions.checkArgument(port >= 0 && port < 65535, "port must be between 0 and 65535.");
        Preconditions.checkArgument(requestTimeoutInMillis > 0, "request_timeout must be greater than 0.");
        Preconditions.checkArgument(threadCount > 0, "thread_count must be greater than 0.");
        Preconditions.checkArgument(maxConnectionCount > 0, "max_connection_count must be greater than 0.");
        Preconditions.checkArgument(maxPendingRequests > 0, "max_pending_requests must be greater than 0.");
        if (ssl) {
            validateFilePath(String.format("%s is enabled", SSL), sslCertificateFile, SSL_CERTIFICATE_FILE);
            validateFilePath(String.format("%s is enabled", SSL), sslKeyFile, SSL_KEY_FILE);
        }

    }

    private void validateFilePath(final String typeMessage, final String argument, final String argumentName) {
        if (StringUtils.isEmpty(argument) || !Files.exists(Path.of(argument))) {
            throw new IllegalArgumentException(String.format("%s, %s needs to be a valid file path.", typeMessage, argumentName));
        }
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
}
