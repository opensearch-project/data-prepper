/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderProviderTest {

    @Mock
    private PeerForwarderClientFactory peerForwarderClientFactory;
    @Mock
    private PeerForwarderClient peerForwarderClient;

    @Mock
    private HashRing hashRing;

    private String pipelineName;
    private String pluginId;
    private Set<String> identificationKeys;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();
        identificationKeys = Collections.singleton(UUID.randomUUID().toString());


        when(peerForwarderClientFactory.createHashRing()).thenReturn(hashRing);
    }

    private PeerForwarderProvider createObjectUnderTest() {
        return new PeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient);
    }

    @Test
    void register_creates_a_new_RemotePeerForwarder() {
        final PeerForwarder peerForwarder = createObjectUnderTest().register(pipelineName, pluginId, identificationKeys);

        assertThat(peerForwarder, instanceOf(RemotePeerForwarder.class));
    }

    @Test
    void register_creates_HashRing() {
        createObjectUnderTest().register(pipelineName, pluginId, identificationKeys);

        verify(peerForwarderClientFactory).createHashRing();
    }

    @Test
    void register_called_multiple_times_creates_only_one_HashRing() {
        final PeerForwarderProvider objectUnderTest = createObjectUnderTest();

        for (int i = 0; i < 10; i++)
            objectUnderTest.register(pipelineName, pluginId, identificationKeys);

        verify(peerForwarderClientFactory, times(1)).createHashRing();
    }
}