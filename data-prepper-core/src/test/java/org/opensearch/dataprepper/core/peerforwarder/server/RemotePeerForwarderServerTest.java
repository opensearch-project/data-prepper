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
import org.opensearch.dataprepper.core.peerforwarder.server.RemotePeerForwarderServer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RemotePeerForwarderServerTest {

    @Mock
    Server server;

    @Mock
    PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    CompletableFuture<Void> completableFuture;

    private RemotePeerForwarderServer createObjectUnderTest() {
        return new RemotePeerForwarderServer(peerForwarderConfiguration, server);
    }

    @Test
    void start_should_invoke_server_start() throws ExecutionException, InterruptedException {
        when(server.start()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();
        remotePeerForwarderServer.start();

        verify(server).start();
    }

    @Test
    void start_should_throw_if_future_completed_exceptionally() throws ExecutionException, InterruptedException {
        when(server.start()).thenReturn(completableFuture);
        when(completableFuture.get()).thenThrow(ExecutionException.class);
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();

        assertThrows(RuntimeException.class, remotePeerForwarderServer::start);
    }

    @Test
    void start_should_throw_if_current_thread_is_interrupted() throws ExecutionException, InterruptedException {
        when(server.start()).thenReturn(completableFuture);
        when(completableFuture.get()).thenThrow(InterruptedException.class);
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();

        assertThrows(RuntimeException.class, remotePeerForwarderServer::start);
    }

    @Test
    void stop_should_invoke_server_stop() throws ExecutionException, InterruptedException {
        when(server.stop()).thenReturn(completableFuture);
        when(completableFuture.get()).thenReturn(mock(Void.class));
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();
        remotePeerForwarderServer.stop();

        verify(server).stop();
    }

    @Test
    void stop_should_throw_if_future_completed_exceptionally() throws ExecutionException, InterruptedException {
        when(server.stop()).thenReturn(completableFuture);
        when(completableFuture.get()).thenThrow(ExecutionException.class);
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();

        assertThrows(RuntimeException.class, remotePeerForwarderServer::stop);
    }

    @Test
    void stop_should_throw_if_current_thread_is_interrupted() throws ExecutionException, InterruptedException {
        when(server.stop()).thenReturn(completableFuture);
        when(completableFuture.get()).thenThrow(InterruptedException.class);
        final RemotePeerForwarderServer remotePeerForwarderServer = createObjectUnderTest();

        assertThrows(RuntimeException.class, remotePeerForwarderServer::stop);
    }
}