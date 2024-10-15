/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderServerProxyTest {

    @Mock
    private Server server;

    @Mock
    private PeerForwarderHttpServerProvider peerForwarderHttpServerProvider;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    CompletableFuture<Void> completableFuture;

    PeerForwarderServerProxy createObjectUnderTest() {
        return new PeerForwarderServerProxy(peerForwarderHttpServerProvider, peerForwarderConfiguration, peerForwarderProvider);
    }

    @Test
    void start_should_start_server_if_peers_are_registered() throws ExecutionException, InterruptedException {
        when(peerForwarderHttpServerProvider.get()).thenReturn(server);
        when(server.start()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));
        when(peerForwarderProvider.isPeerForwardingRequired()).thenReturn(true);

        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
        verify(peerForwarderHttpServerProvider).get();
        verify(server).start();
    }

    @Test
    void start_should_not_start_server_if_no_peers_are_registered() {
        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();

        verifyNoInteractions(server);
    }

    @Test
    void stop_should_not_stop_server_if_server_is_not_started() {
        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.stop();
        verifyNoInteractions(server);
    }

    @Test
    void stop_should_stop_server_if_server_started() throws ExecutionException, InterruptedException {
        when(peerForwarderHttpServerProvider.get()).thenReturn(server);
        when(server.start()).thenReturn(completableFuture);
        when(server.stop()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));
        when(peerForwarderProvider.isPeerForwardingRequired()).thenReturn(true);

        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
        objectUnderTest.stop();
        verify(peerForwarderHttpServerProvider).get();
        verify(server).stop();
    }

    @Test
    void no_server_interaction_if_peer_forwarding_not_required() {
        when(peerForwarderProvider.isPeerForwardingRequired()).thenReturn(false);

        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
        objectUnderTest.stop();
        verifyNoInteractions(server);
    }

}