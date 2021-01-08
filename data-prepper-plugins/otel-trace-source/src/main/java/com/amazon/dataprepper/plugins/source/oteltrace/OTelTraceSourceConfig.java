package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.model.configuration.PluginSetting;

public class OTelTraceSourceConfig {
    public static final String REQUEST_TIMEOUT = "request_timeout";
    public static final String PORT = "port";
    public static final String SSL = "ssl";
    public static final String HEALTH_CHECK_SERVICE = "health_check_service";
    public static final String PROTO_REFLECTION_SERVICE = "proto_reflection_service";
    public static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    public static final String SSL_KEY_FILE = "sslKeyFile";
    private static final int DEFAULT_REQUEST_TIMEOUT = 10_000;
    private static final int DEFAULT_PORT = 21890;
    private static final boolean DEFAULT_SSL = false;
    private final int requestTimeoutInMillis;
    private final int port;
    private final boolean healthCheck;
    private final boolean protoReflectionService;
    private final boolean ssl;
    private final String sslKeyCertChainFile;
    private final String sslKeyFile;


    private OTelTraceSourceConfig(final int requestTimeoutInMillis,
                                  final int port,
                                  final boolean healthCheck,
                                  final boolean protoReflectionService,
                                  final boolean isSSL,
                                  final String sslKeyCertChainFile,
                                  final String sslKeyFile) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.port = port;
        this.healthCheck = healthCheck;
        this.protoReflectionService = protoReflectionService;
        this.ssl = isSSL;
        this.sslKeyCertChainFile = sslKeyCertChainFile;
        this.sslKeyFile = sslKeyFile;
        if (ssl && (sslKeyCertChainFile == null || sslKeyCertChainFile.isEmpty())) {
            throw new IllegalArgumentException(String.format("%s is enable, %s can not be empty or null", SSL, SSL_KEY_CERT_FILE));
        }
        if (ssl && (sslKeyFile == null || sslKeyFile.isEmpty())) {
            throw new IllegalArgumentException(String.format("%s is enable, %s can not be empty or null", SSL, SSL_KEY_CERT_FILE));
        }
    }

    public static OTelTraceSourceConfig buildConfig(final PluginSetting pluginSetting) {
        return new OTelTraceSourceConfig(pluginSetting.getIntegerOrDefault(REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT),
                pluginSetting.getIntegerOrDefault(PORT, DEFAULT_PORT),
                pluginSetting.getBooleanOrDefault(HEALTH_CHECK_SERVICE, false),
                pluginSetting.getBooleanOrDefault(PROTO_REFLECTION_SERVICE, false),
                pluginSetting.getBooleanOrDefault(SSL, DEFAULT_SSL),
                pluginSetting.getStringOrDefault(SSL_KEY_CERT_FILE, null),
                pluginSetting.getStringOrDefault(SSL_KEY_FILE, null));
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
}
