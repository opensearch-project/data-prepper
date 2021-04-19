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

public class OTelTraceSourceConfig {
    static final String REQUEST_TIMEOUT = "request_timeout";
    static final String PORT = "port";
    static final String SSL = "ssl";
    static final String HEALTH_CHECK_SERVICE = "health_check_service";
    static final String PROTO_REFLECTION_SERVICE = "proto_reflection_service";
    static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    static final String SSL_KEY_FILE = "sslKeyFile";
    static final String THREAD_COUNT = "thread_count";
    static final String MAX_CONNECTION_COUNT = "max_connection_count";
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 10000;
    static final int DEFAULT_PORT = 21890;
    static final int DEFAULT_THREAD_COUNT = 200;
    static final int DEFAULT_MAX_CONNECTION_COUNT = 500;
    static final boolean DEFAULT_SSL = true;
    private final int requestTimeoutInMillis;
    private final int port;
    private final boolean healthCheck;
    private final boolean protoReflectionService;
    private final boolean ssl;
    private final String sslKeyCertChainFile;
    private final String sslKeyFile;
    private final int threadCount;
    private final int maxConnectionCount;

    private OTelTraceSourceConfig(final int requestTimeoutInMillis,
                                  final int port,
                                  final boolean healthCheck,
                                  final boolean protoReflectionService,
                                  final boolean isSSL,
                                  final String sslKeyCertChainFile,
                                  final String sslKeyFile,
                                  final int threadCount,
                                  final int maxConnectionCount) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.port = port;
        this.healthCheck = healthCheck;
        this.protoReflectionService = protoReflectionService;
        this.ssl = isSSL;
        this.sslKeyCertChainFile = sslKeyCertChainFile;
        this.sslKeyFile = sslKeyFile;
        this.threadCount = threadCount;
        this.maxConnectionCount = maxConnectionCount;
        if (ssl && (sslKeyCertChainFile == null || sslKeyCertChainFile.isEmpty())) {
            throw new IllegalArgumentException(String.format("%s is enabled, %s can not be empty or null", SSL, SSL_KEY_CERT_FILE));
        }
        if (ssl && (sslKeyFile == null || sslKeyFile.isEmpty())) {
            throw new IllegalArgumentException(String.format("%s is enabled, %s can not be empty or null", SSL, SSL_KEY_CERT_FILE));
        }
    }

    public static OTelTraceSourceConfig buildConfig(final PluginSetting pluginSetting) {
        return new OTelTraceSourceConfig(pluginSetting.getIntegerOrDefault(REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_MS),
                pluginSetting.getIntegerOrDefault(PORT, DEFAULT_PORT),
                pluginSetting.getBooleanOrDefault(HEALTH_CHECK_SERVICE, false),
                pluginSetting.getBooleanOrDefault(PROTO_REFLECTION_SERVICE, false),
                pluginSetting.getBooleanOrDefault(SSL, DEFAULT_SSL),
                pluginSetting.getStringOrDefault(SSL_KEY_CERT_FILE, null),
                pluginSetting.getStringOrDefault(SSL_KEY_FILE, null),
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

    public boolean isSsl() {
        return ssl;
    }

    public String getSslKeyCertChainFile() {
        return sslKeyCertChainFile;
    }

    public String getSslKeyFile() {
        return sslKeyFile;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getMaxConnectionCount() {
        return maxConnectionCount;
    }
}
