package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProviderConfig;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.CertificateProviderFactory;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery.PeerListProvider;
import com.amazon.dataprepper.plugins.prepper.peerforwarder.discovery.PeerListProviderFactory;
import org.apache.commons.lang3.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;

public class PeerForwarderConfig {
    public static final String TIME_OUT = "time_out";
    public static final String MAX_NUM_SPANS_PER_REQUEST = "span_agg_count";
    public static final int NUM_VIRTUAL_NODES = 10;
    public static final String DISCOVERY_MODE = "discovery_mode";
    public static final String DOMAIN_NAME = "domain_name";
    public static final String STATIC_ENDPOINTS = "static_endpoints";
    public static final String SSL = "ssl";
    public static final String SSL_KEY_CERT_FILE = "sslKeyCertChainFile";
    private static final boolean DEFAULT_SSL = true;
    private static final String USE_ACM_CERT_FOR_SSL = "useAcmCertForSSL";
    private static final boolean DEFAULT_USE_ACM_CERT_FOR_SSL = false;
    private static final String ACM_CERT_ISSUE_TIME_OUT_MILLIS = "acmCertIssueTimeOutMillis";
    private static final int DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS = 120000;
    private static final String ACM_CERT_ARN = "acmCertificateArn";
    public static final String AWS_REGION = "awsRegion";
    public static final String AWS_CLOUD_MAP_NAMESPACE_NAME = "awsCloudMapNamespaceName";
    public static final String AWS_CLOUD_MAP_SERVICE_NAME = "awsCloudMapServiceName";

    private final HashRing hashRing;
    private final PeerClientPool peerClientPool;
    private final int timeOut;
    private final int maxNumSpansPerRequest;

    private PeerForwarderConfig(final PeerClientPool peerClientPool,
                                final HashRing hashRing,
                                final int timeOut,
                                final int maxNumSpansPerRequest) {
        checkNotNull(peerClientPool);
        checkNotNull(hashRing);

        this.peerClientPool = peerClientPool;
        this.hashRing = hashRing;
        this.timeOut = timeOut;
        this.maxNumSpansPerRequest = maxNumSpansPerRequest;
    }

    public static PeerForwarderConfig buildConfig(final PluginSetting pluginSetting) {
        final PeerListProvider peerListProvider = new PeerListProviderFactory().createProvider(pluginSetting);
        final HashRing hashRing = new HashRing(peerListProvider, NUM_VIRTUAL_NODES);
        final PeerClientPool peerClientPool = PeerClientPool.getInstance();
        peerClientPool.setClientTimeoutSeconds(3);
        final boolean ssl = pluginSetting.getBooleanOrDefault(SSL, DEFAULT_SSL);
        final String sslKeyCertChainFilePath = pluginSetting.getStringOrDefault(SSL_KEY_CERT_FILE, null);
        final boolean useAcmCertForSsl = pluginSetting.getBooleanOrDefault(USE_ACM_CERT_FOR_SSL, DEFAULT_USE_ACM_CERT_FOR_SSL);

        if (ssl || useAcmCertForSsl) {
            if (ssl && StringUtils.isEmpty(sslKeyCertChainFilePath)) {
                throw new IllegalArgumentException(String.format("%s is enabled, %s can not be empty or null", SSL, SSL_KEY_CERT_FILE));
            }
            peerClientPool.setSsl(true);
            final String acmCertificateArn = pluginSetting.getStringOrDefault(ACM_CERT_ARN, null);
            final long acmCertIssueTimeOutMillis = pluginSetting.getLongOrDefault(ACM_CERT_ISSUE_TIME_OUT_MILLIS, DEFAULT_ACM_CERT_ISSUE_TIME_OUT_MILLIS);
            final String awsRegion = pluginSetting.getStringOrDefault(AWS_REGION, null);
            final CertificateProviderConfig certificateProviderConfig = new CertificateProviderConfig(
                    useAcmCertForSsl,
                    acmCertificateArn,
                    awsRegion,
                    acmCertIssueTimeOutMillis,
                    sslKeyCertChainFilePath
            );
            final CertificateProviderFactory certificateProviderFactory = new CertificateProviderFactory(certificateProviderConfig);
            peerClientPool.setCertificate(certificateProviderFactory.getCertificateProvider().getCertificate());

        }

        return new PeerForwarderConfig(
                peerClientPool,
                hashRing,
                pluginSetting.getIntegerOrDefault(TIME_OUT, 3),
                pluginSetting.getIntegerOrDefault(MAX_NUM_SPANS_PER_REQUEST, 48));
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
}
