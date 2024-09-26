/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server;

import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class DataPrepperServerTest {
    @Mock
    private HttpServer server;

    @Mock
    private HttpServerProvider httpServerProvider;

    @Mock
    private ListPipelinesHandler listPipelinesHandler;

    @Mock
    private ShutdownHandler shutdownHandler;

    @Mock
    private GetTransformedPipelinesBodyHandler getTransformedPipelinesBodyHandler;

    @Mock
    private PrometheusMeterRegistry prometheusMeterRegistry;

    @Mock
    private HttpContext context;

    @Mock
    private InetSocketAddress socketAddress;

    @Mock
    private Authenticator authenticator;

    @AfterEach
    public void tearDown() {
        verifyNoMoreInteractions(server, httpServerProvider, listPipelinesHandler, shutdownHandler,
                prometheusMeterRegistry, authenticator, context, socketAddress);
    }

    @Test
    public void testDataPrepperServerConstructor() {
        final DataPrepperServer dataPrepperServer = createObjectUnderTest(prometheusMeterRegistry, authenticator);
        assertThat(dataPrepperServer, is(notNullValue()));
    }

    @Test
    public void testGivenValidServerWhenStartThenShouldCallServerStart() {
        mockServerStart();

        final DataPrepperServer dataPrepperServer = createObjectUnderTest(prometheusMeterRegistry, authenticator);
        dataPrepperServer.start();

        verifyServerStart();
        verify(server).createContext(eq("/metrics/prometheus"), any(PrometheusMetricsHandler.class));
        verify(server).createContext(eq("/metrics/sys"), any(PrometheusMetricsHandler.class));
        verify(context, times(5)).setAuthenticator(eq(authenticator));
    }

    @Test
    public void testGivenValidServerWhenStartThenShouldCallServerStart_NullPrometheus() {
        mockServerStart();

        final DataPrepperServer dataPrepperServer = createObjectUnderTest(null, authenticator);
        dataPrepperServer.start();

        verifyServerStart();
        verify(context, times(3)).setAuthenticator(eq(authenticator));
    }

    @Test
    public void testGivenValidServerWhenStartThenShouldCallServerStart_NullAuthenticator() {
        mockServerStart();

        final DataPrepperServer dataPrepperServer = createObjectUnderTest(prometheusMeterRegistry, null);
        dataPrepperServer.start();

        verifyServerStart();
        verify(server).createContext(eq("/metrics/prometheus"), any(PrometheusMetricsHandler.class));
        verify(server).createContext(eq("/metrics/sys"), any(PrometheusMetricsHandler.class));
    }

    @Test
    public void testGivenValidServerWhenStartThenShouldCallServerStart_NullPrometheusAndAuthenticator() {
        mockServerStart();

        final DataPrepperServer dataPrepperServer = createObjectUnderTest(null, null);
        dataPrepperServer.start();

        verifyServerStart();

    }

    @Test
    public void testGivenValidServerWhenStopThenShouldCallServerStopWithNoDelay() {
        mockServerStart();

        final DataPrepperServer dataPrepperServer = createObjectUnderTest(null, null);
        dataPrepperServer.start();
        dataPrepperServer.stop();

        verifyServerStart();
        verify(server).stop(eq(0));
    }

    private void mockServerStart() {
        when(httpServerProvider.get())
                .thenReturn(server);

        when(server.getAddress())
                .thenReturn(socketAddress);
        when(server.createContext(any(String.class), any(HttpHandler.class)))
                .thenReturn(context);
    }

    private void verifyServerStart() {
        verify(httpServerProvider).get();
        verify(server).createContext("/list", listPipelinesHandler);
        verify(server).createContext(eq("/shutdown"), eq(shutdownHandler));
        verify(server).createContext(eq("/getPipelineBody"), eq(getTransformedPipelinesBodyHandler));
        final ArgumentCaptor<ExecutorService> executorServiceArgumentCaptor = ArgumentCaptor.forClass(ExecutorService.class);
        verify(server).setExecutor(executorServiceArgumentCaptor.capture());
        final ExecutorService actualExecutorService = executorServiceArgumentCaptor.getValue();
        assertThat(actualExecutorService, instanceOf(ThreadPoolExecutor.class));
        final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) actualExecutorService;
        assertThat(threadPoolExecutor.getMaximumPoolSize(), greaterThanOrEqualTo(3));

        verify(server).start();
        verify(server).getAddress();
        verify(socketAddress).getPort();
    }

    private DataPrepperServer createObjectUnderTest(final PrometheusMeterRegistry prometheusMeterRegistry, final Authenticator authenticator) {
        return new DataPrepperServer(httpServerProvider, listPipelinesHandler, shutdownHandler, getTransformedPipelinesBodyHandler, prometheusMeterRegistry, authenticator);
    }
}
