/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PeerForwarderServerProxyTest {

    @Mock
    Server server;

    @Mock
    PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    CompletableFuture<Void> completableFuture;

    PeerForwarderServerProxy createObjectUnderTest() {
        return new PeerForwarderServerProxy(peerForwarderConfiguration, server);
    }

    @Test
    void start_should_start_server_if_peers_configured() throws ExecutionException, InterruptedException {
        when(server.start()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));

        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
        verify(server).start();
    }

    @Test
    void stop_should_not_stop_server_if_server_is_not_started() throws ExecutionException, InterruptedException {
        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.stop();
        verifyNoInteractions(server);
    }

    @Test
    void stop_should_stop_server_if_server_started() throws ExecutionException, InterruptedException {
        when(server.start()).thenReturn(completableFuture);
        when(server.stop()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));

        final PeerForwarderServerProxy objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
        objectUnderTest.stop();
        verify(server).stop();
    }

}