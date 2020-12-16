package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.DiscoveryMode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PeerForwarderConfigTest {
    private static final String VALID_SSL_KEY_CERT_FILE = PeerForwarderConfigTest.class.getClassLoader()
            .getResource("test-crt.crt").getFile();
    private static final String INVALID_SSL_KEY_CERT_FILE = "";
    private static List<String> TEST_ENDPOINTS = Arrays.asList("172.0.0.1", "172.0.0.2");

    @Test
    public void testBuildConfigNoSSL() {
        final List<String> testPeerIps = Arrays.asList("172.0.0.1", "172.0.0.2");
        final int testNumSpansPerRequest = 2;
        final int testTimeout = 300;
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        settings.put(PeerForwarderConfig.STATIC_ENDPOINTS, testPeerIps);
        settings.put(PeerForwarderConfig.MAX_NUM_SPANS_PER_REQUEST, testNumSpansPerRequest);
        settings.put(PeerForwarderConfig.TIME_OUT, testTimeout);

        final PeerForwarderConfig peerForwarderConfig = PeerForwarderConfig.buildConfig(
                new PluginSetting("peer_forwarder", settings));

        Assert.assertEquals(testNumSpansPerRequest, peerForwarderConfig.getMaxNumSpansPerRequest());
        Assert.assertEquals(testTimeout, peerForwarderConfig.getTimeOut());
    }

    @Test
    public void testBuildConfigInvalidSSL() {
        final List<String> testPeerIps = Arrays.asList("172.0.0.1", "172.0.0.2");
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        settings.put(PeerForwarderConfig.STATIC_ENDPOINTS, testPeerIps);
        settings.put(PeerForwarderConfig.SSL, true);

        settings.put(PeerForwarderConfig.SSL_KEY_CERT_FILE, INVALID_SSL_KEY_CERT_FILE);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PeerForwarderConfig.buildConfig(new PluginSetting("peer_forwarder", settings));
        });

        settings.put(PeerForwarderConfig.SSL_KEY_CERT_FILE, null);
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            PeerForwarderConfig.buildConfig(new PluginSetting("peer_forwarder", settings));
        });
    }

    @Test
    public void testBuildConfigValidSSL() {
        final List<String> testPeerIps = Arrays.asList("172.0.0.1", "172.0.0.2");
        final HashMap<String, Object> settings = new HashMap<>();
        settings.put(PeerForwarderConfig.DISCOVERY_MODE, DiscoveryMode.STATIC.toString());
        settings.put(PeerForwarderConfig.STATIC_ENDPOINTS, testPeerIps);
        settings.put(PeerForwarderConfig.SSL, true);
        settings.put(PeerForwarderConfig.SSL_KEY_CERT_FILE, VALID_SSL_KEY_CERT_FILE);

        final PeerClientPool mockedPeerClientPool = Mockito.mock(PeerClientPool.class);
        doNothing().when(mockedPeerClientPool).setSsl(anyBoolean());
        doNothing().when(mockedPeerClientPool).setSslKeyCertChainFile(any(File.class));
        final MockedStatic<PeerClientPool> mockedPeerClientPoolClass = Mockito.mockStatic(PeerClientPool.class);
        mockedPeerClientPoolClass.when(PeerClientPool::getInstance).thenReturn(mockedPeerClientPool);
        final PeerForwarderConfig peerForwarderConfig = PeerForwarderConfig.buildConfig(new PluginSetting("peer_forwarder", settings));
        verify(mockedPeerClientPool, times(1)).setSsl(true);
        verify(mockedPeerClientPool, times(1)).setSslKeyCertChainFile(new File(VALID_SSL_KEY_CERT_FILE));
    }
}