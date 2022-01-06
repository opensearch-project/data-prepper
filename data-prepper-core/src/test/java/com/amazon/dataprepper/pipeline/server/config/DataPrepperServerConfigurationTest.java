/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server.config;

import com.amazon.dataprepper.parser.model.DataPrepperConfiguration;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperServerConfigurationTest {
    private static final Random r = new Random();
    private static final Integer MAX_PORT = 65535;
    private static final String P12_KEYSTORE = "src/test/resources/tls/test_keystore.p12";

    private static boolean available(final int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (final IOException ignored) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (final IOException ignored) {
                }
            }
        }

        return false;
    }

    private static int getPort() {
        final int MAX_RETRIES = 10;
        for (int i = 0; i < MAX_RETRIES; i++) {
            final int electivePort = r.nextInt(MAX_PORT);
            if (available(electivePort)) {
                return electivePort;
            }
        }
        throw new RuntimeException("No available port found");
    }

    @Mock
    private DataPrepperConfiguration configuration;

    private final DataPrepperServerConfiguration serverConfiguration = new DataPrepperServerConfiguration();

    @Test
    public void testGivenNoSslThenInsecureServerCreated() {
        final int expectedPort = getPort();
        when(configuration.ssl())
                .thenReturn(false);
        when(configuration.getServerPort())
                .thenReturn(expectedPort);

        HttpServer server = serverConfiguration.httpServer(configuration);

        assertThat(server, isA(HttpServer.class));
        assertThat(server, not(isA(HttpsServer.class)));
        assertThat(server.getAddress().getPort(), is(expectedPort));
    }

    @Test
    public void testGivenSslConfigThenHttpsServerCreater() {
        final int expectedPort = getPort();

        when(configuration.ssl())
                .thenReturn(true);
        when(configuration.getKeyStoreFilePath())
                .thenReturn(P12_KEYSTORE);
        when(configuration.getKeyStorePassword())
                .thenReturn("");
        when(configuration.getPrivateKeyPassword())
                .thenReturn("");
        when(configuration.getServerPort())
                .thenReturn(expectedPort);

        HttpServer server = serverConfiguration.httpServer(configuration);

        assertThat(server, isA(HttpServer.class));
        assertThat(server, isA(HttpsServer.class));
        assertThat(server.getAddress().getPort(), is(expectedPort));

        HttpsServer httpsServer = (HttpsServer) server;

        assertThat(httpsServer.getHttpsConfigurator(), isA(HttpsConfigurator.class));

        verify(configuration, times(1)).getKeyStoreFilePath();
        verify(configuration, times(1)).getKeyStorePassword();
        verify(configuration, times(1)).getPrivateKeyPassword();
    }

    @Test
    public void testGivenPortInUseThenExceptionThrown() throws IOException {
        final int port = getPort();
        InetSocketAddress socketAddress = new InetSocketAddress(port);

        HttpServer portBlockingServer = HttpServer.create(socketAddress, 0);
        portBlockingServer.start();

        when(configuration.getServerPort())
                .thenReturn(port);

        assertThrows(RuntimeException.class, () -> serverConfiguration.httpServer(configuration));
    }
}