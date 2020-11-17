package com.amazon.situp.plugins.source.oteltrace;

import com.amazon.situp.model.configuration.PluginSetting;

public class OTelTraceSourceConfig {
    private static final String REQUEST_TIMEOUT = "request_timeout";
    private static final String PORT = "port";
    private static final String SSL = "ssl";
    private static final String HEALTH_CHECK = "health_check";
    private static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    private static final String SSL_KEY_FILE = "sslKeyFile";
    private static final int DEFAULT_REQUEST_TIMEOUT = 10_000;
    private static final int DEFAULT_PORT = 21890;
    private static final boolean DEFAULT_SSL = false;
    private final int requestTimeoutInMillis;
    private final int port;
    private final boolean healthCheck;
    private final boolean ssl;
    private final String sslKeyCertChainFile;
    private final String sslKeyFile;


    private OTelTraceSourceConfig(int requestTimeoutInMillis, int port, boolean health_check, boolean isSSL, String sslKeyCertChainFile, String sslKeyFile) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        this.port = port;
        this.healthCheck = health_check;
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
                pluginSetting.getBooleanOrDefault(HEALTH_CHECK, false),
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

    public boolean isHealthCheck() {
        return healthCheck;
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
