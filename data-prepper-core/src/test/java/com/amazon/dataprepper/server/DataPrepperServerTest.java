/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.server;

import com.amazon.dataprepper.pipeline.server.DataPrepperServer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class DataPrepperServerTest {
    @Mock
    private HttpServer server;

    @InjectMocks
    private DataPrepperServer dataPrepperServer;

    @Test
    public void testDataPrepperServerConstructor() {
        assertThat(dataPrepperServer, is(notNullValue()));
    }

    @Test
    public void testGivenValidServerWhenStartThenShouldCallServerStart() {
        final InetSocketAddress socketAddress = mock(InetSocketAddress.class);

        when(server.getAddress())
                .thenReturn(socketAddress);

        dataPrepperServer.start();

        verify(server).start();
        verify(server).getAddress();
        verify(socketAddress).getPort();
    }

    @Test
    public void testGivenValidServerWhenStopThenShouldCallServerStopWithNoDely() {
        dataPrepperServer.stop();

        verify(server).stop(eq(0));
    }
}
