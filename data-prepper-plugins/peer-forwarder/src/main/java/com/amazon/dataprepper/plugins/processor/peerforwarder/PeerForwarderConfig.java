package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.PeerListProvider;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.PeerListProviderFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class PeerForwarderConfig {
    public static final String TIME_OUT = "time_out";
    public static final String MAX_NUM_SPANS_PER_REQUEST = "span_agg_count";
    public static final int NUM_VIRTUAL_NODES = 10;
    public static final String DISCOVERY_MODE = "discovery_mode";
    public static final String HOSTNAME_FOR_DNS_LOOKUP = "hostname_for_dns_lookup";
    public static final String STATIC_ENDPOINTS = "static_endpoints";
    public static final String SSL = "ssl";
    public static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    public static final String SSL_KEY_FILE = "sslKeyFile";
    private static final boolean DEFAULT_SSL = false;

    private final HashRing hashRing;
    private final PeerClientPool peerClientPool;
    private final int timeOut;
    private final int maxNumSpansPerRequest;
    private final boolean ssl;
    private final String sslKeyCertChainFile;
    private final String sslKeyFile;

    private PeerForwarderConfig(final PeerClientPool peerClientPool,
                                final HashRing hashRing,
                                final int timeOut,
                                final int maxNumSpansPerRequest,
                                final boolean isSSL,
                                final String sslKeyCertChainFile,
                                final String sslKeyFile) {
        checkNotNull(peerClientPool);
        checkNotNull(hashRing);

        this.peerClientPool = peerClientPool;
        this.hashRing = hashRing;
        this.timeOut = timeOut;
        this.maxNumSpansPerRequest = maxNumSpansPerRequest;
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

    public static PeerForwarderConfig buildConfig(final PluginSetting pluginSetting) {
        final PeerListProvider peerListProvider = new PeerListProviderFactory().createProvider(pluginSetting);
        final HashRing hashRing = new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
        final PeerClientPool peerClientPool = PeerClientPool.getInstance();
        peerClientPool.setClientTimeoutSeconds(3);

        return new PeerForwarderConfig(
                peerClientPool,
                hashRing,
                pluginSetting.getIntegerOrDefault(TIME_OUT, 3),
                pluginSetting.getIntegerOrDefault(MAX_NUM_SPANS_PER_REQUEST, 48),
                pluginSetting.getBooleanOrDefault(SSL, DEFAULT_SSL),
                pluginSetting.getStringOrDefault(SSL_KEY_CERT_FILE, null),
                pluginSetting.getStringOrDefault(SSL_KEY_FILE, null));
    }

    public HashRing getHashRing() {
        return hashRing;
    }

    public PeerClientPool getPeerClientPool() {
        return peerClientPool;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getMaxNumSpansPerRequest() {
        return maxNumSpansPerRequest;
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
