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

import java.io.File;
import java.io.IOException;

import static com.amazon.dataprepper.TestDataProvider.VALID_PEER_FORWARDER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_DEFAULT_PEER_FORWARDER_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INVALID_PEER_FORWARDER_CONFIG_FILE;
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
        final PeerForwarderConfiguration peerForwarderConfiguration = makeConfig(VALID_DEFAULT_PEER_FORWARDER_CONFIG_FILE);

        assertThat(peerForwarderConfiguration.getServerPort(), equalTo(21890));
        assertThat(peerForwarderConfiguration.getRequestTimeout(), equalTo(10_000));
        assertThat(peerForwarderConfiguration.getThreadCount(), equalTo(200));
        assertThat(peerForwarderConfiguration.getMaxConnectionCount(), equalTo(500));
        assertThat(peerForwarderConfiguration.getMaxPendingRequests(), equalTo(1024));
        assertThat(peerForwarderConfiguration.isSsl(), equalTo(true));
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
        assertThat(peerForwarderConfiguration.getDiscoveryMode(), equalTo(DiscoveryMode.STATIC));
        assertThat(peerForwarderConfiguration.getBatchSize(), equalTo(100));
        assertThat(peerForwarderConfiguration.getBufferSize(), equalTo(100));
    }

    @Test
    void testInvalidPeerForwarderConfig() {
        assertThrows(ValueInstantiationException.class, () -> makeConfig(INVALID_PEER_FORWARDER_CONFIG_FILE));
    }
}