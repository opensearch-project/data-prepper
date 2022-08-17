/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.peerforwarder;

import com.amazon.dataprepper.peerforwarder.discovery.DiscoveryMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static com.amazon.dataprepper.TestDataProvider.VALID_PEER_FORWARDER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_PEER_FORWARDER_CONFIG_WITHOUT_SSL_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_PORT_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_SSL_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_BUFFER_SIZE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_CONNECTION_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_BATCH_SIZE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_THREAD_COUNT_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_DISCOVERY_MODE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_ARN_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_REGION_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_SERVICE_NAME_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_NAMESPACE_NAME_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_REGION_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_WITH_DNS_WITHOUT_DOMAIN_NAME_CONFIG_FILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PeerForwarderConfigurationTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    private static PeerForwarderConfiguration makeConfig(final String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        return OBJECT_MAPPER.readValue(configurationFile, PeerForwarderConfiguration.class);
    }

    @Test
    void testPeerForwarderDefaultConfig() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(VALID_PEER_FORWARDER_CONFIG_WITHOUT_SSL_FILE);

        assertThat(peerForwarderConfiguration.getServerPort(), equalTo(21890));
        assertThat(peerForwarderConfiguration.getRequestTimeout(), equalTo(10_000));
        assertThat(peerForwarderConfiguration.getThreadCount(), equalTo(200));
        assertThat(peerForwarderConfiguration.getMaxConnectionCount(), equalTo(500));
        assertThat(peerForwarderConfiguration.getMaxPendingRequests(), equalTo(1024));
        assertThat(peerForwarderConfiguration.isSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getAcmPrivateKeyPassword(), equalTo(null));
        assertThat(peerForwarderConfiguration.isUseAcmCertificateForSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getDiscoveryMode(), equalTo(DiscoveryMode.STATIC));
        assertThat(peerForwarderConfiguration.getBatchSize(), equalTo(48));
        assertThat(peerForwarderConfiguration.getBufferSize(), equalTo(512));
    }

    @Test
    void testValidPeerForwarderConfig() throws IOException {
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(VALID_PEER_FORWARDER_CONFIG_FILE);

        assertThat(peerForwarderConfiguration.getServerPort(), equalTo(21895));
        assertThat(peerForwarderConfiguration.getRequestTimeout(), equalTo(1000));
        assertThat(peerForwarderConfiguration.getThreadCount(), equalTo(100));
        assertThat(peerForwarderConfiguration.getMaxConnectionCount(), equalTo(100));
        assertThat(peerForwarderConfiguration.getMaxPendingRequests(), equalTo(512));
        assertThat(peerForwarderConfiguration.isSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.isUseAcmCertificateForSsl(), equalTo(false));
        assertThat(peerForwarderConfiguration.getAcmCertificateArn(), equalTo(null));
        assertThat(peerForwarderConfiguration.getDiscoveryMode(), equalTo(DiscoveryMode.STATIC));
        assertThat(peerForwarderConfiguration.getStaticEndpoints(), equalTo(new ArrayList<>()));
        assertThat(peerForwarderConfiguration.getDomainName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getAwsCloudMapNamespaceName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getAwsCloudMapServiceName(), equalTo(null));
        assertThat(peerForwarderConfiguration.getBatchSize(), equalTo(100));
        assertThat(peerForwarderConfiguration.getBufferSize(), equalTo(100));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            INVALID_PEER_FORWARDER_WITH_PORT_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_THREAD_COUNT_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_CONNECTION_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_SSL_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_DISCOVERY_MODE_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_BUFFER_SIZE_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_BATCH_SIZE_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_ARN_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_ACM_WITHOUT_REGION_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_SERVICE_NAME_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_NAMESPACE_NAME_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_CLOUD_MAP_WITHOUT_REGION_CONFIG_FILE,
            INVALID_PEER_FORWARDER_WITH_DNS_WITHOUT_DOMAIN_NAME_CONFIG_FILE
    })
    void invalid_InvalidPeerForwarderConfig_test(final String filePath) {
        assertThrows(ValueInstantiationException.class, () -> makeConfig(filePath));
    }
}