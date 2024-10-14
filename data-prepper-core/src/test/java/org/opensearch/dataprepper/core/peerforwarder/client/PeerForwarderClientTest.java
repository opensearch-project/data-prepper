/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.client;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.core.peerforwarder.PeerClientPool;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderClientFactory;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;
import static org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient.REQUESTS;
import static org.opensearch.dataprepper.core.peerforwarder.client.PeerForwarderClient.CLIENT_REQUEST_FORWARDING_LATENCY;

@ExtendWith(MockitoExtension.class)
class PeerForwarderClientTest {

    private static final String LOCAL_IP = "127.0.0.1";
    private static final String TEST_PLUGIN_ID = "test_plugin_id";
    private static final String TEST_PIPELINE_NAME = "test_pipeline_name";
    private static final String TEST_ADDRESS = "test_address";

    @Mock
    private PeerForwarderCodec peerForwarderCodec;
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;
    @Mock
    private PeerClientPool peerClientPool;

    @Mock
    private PeerForwarderClientFactory peerForwarderClientFactory;

    @Mock
    private Counter requestsCounter;
    private NoopTimer clientRequestForwardingLatencyTimer;

    @BeforeEach
    void setUp() throws Exception {
        when(peerForwarderCodec.serialize(any(PeerForwardingEvents.class))).thenReturn(new byte[10]);
        clientRequestForwardingLatencyTimer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        when(pluginMetrics.counter(REQUESTS)).thenReturn(requestsCounter);
        when(pluginMetrics.timer(CLIENT_REQUEST_FORWARDING_LATENCY)).thenReturn(clientRequestForwardingLatencyTimer);

        when(peerForwarderClientFactory.setPeerClientPool()).thenReturn(peerClientPool);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(requestsCounter);
    }

    private PeerForwarderClient createObjectUnderTest() {
        when(peerForwarderConfiguration.getClientThreadCount()).thenReturn(200);
        return new PeerForwarderClient(peerForwarderConfiguration, peerForwarderClientFactory,
                peerForwarderCodec, pluginMetrics);
    }

    @Test
    void test_serializeRecordsAndSendHttpRequest_with_actual_client_and_server_should_return()
            throws ExecutionException, InterruptedException, IOException {
        when(peerForwarderClientFactory.setPeerClientPool()).thenReturn(peerClientPool);

        final HttpServer server = createServer(2022);
        server.createContext(DEFAULT_PEER_FORWARDING_URI, new TestHandler());
        server.start();

        final InetSocketAddress address = server.getAddress();
        final WebClient testClient = getTestClient(String.valueOf(address.getPort()));
        when(peerClientPool.getClient(anyString())).thenReturn(testClient);

        final PeerForwarderClient peerForwarderClient = createObjectUnderTest();

        final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseFuture =
                peerForwarderClient.serializeRecordsAndSendHttpRequest(generateBatchRecords(1), address.toString(),
                TEST_PLUGIN_ID, TEST_PIPELINE_NAME);
        final AggregatedHttpResponse aggregatedHttpResponse = aggregatedHttpResponseFuture.get();

        assertThat(aggregatedHttpResponse, notNullValue());
        assertThat(aggregatedHttpResponse, instanceOf(AggregatedHttpResponse.class));
        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
        server.stop(0);

        verify(requestsCounter).increment();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void test_serializeRecordsAndSendHttpRequest_should_only_call_setPeerClientPool_once_even_with_multiple_calls(final int requestCount) throws ExecutionException, InterruptedException {
        when(peerForwarderClientFactory.setPeerClientPool()).thenReturn(peerClientPool);

        final WebClient webClient = mock(WebClient.class);
        when(peerClientPool.getClient(anyString())).thenReturn(webClient);
        when(webClient.post(anyString(), any(byte[].class))).thenReturn(HttpResponse.ofJson(CompletableFuture.class));

        final PeerForwarderClient peerForwarderClient = createObjectUnderTest();
        final Collection<Record<Event>> records = generateBatchRecords(1);

        for (int i = 0; i < requestCount; i++) {
            final CompletableFuture<AggregatedHttpResponse> aggregatedHttpResponseFuture =
                    peerForwarderClient.serializeRecordsAndSendHttpRequest(records, TEST_ADDRESS,
                            TEST_PLUGIN_ID, TEST_PIPELINE_NAME);
            final AggregatedHttpResponse aggregatedHttpResponse = aggregatedHttpResponseFuture.get();
            assertThat(aggregatedHttpResponse, notNullValue());
            assertThat(aggregatedHttpResponse, instanceOf(AggregatedHttpResponse.class));
            assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
        }

        verify(requestsCounter, times(requestCount)).increment();
        verify(peerForwarderClientFactory).setPeerClientPool();
    }

    private Collection<Record<Event>> generateBatchRecords(final int numRecords) {
        final Collection<Record<Event>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value" + i);
            eventData.put("key2", "value" + i);
            final JacksonEvent event = JacksonLog.builder().withData(eventData).build();
            results.add(new Record<>(event));
        }
        return results;
    }

    private WebClient getTestClient(final String port) {

        ClientBuilder clientBuilder = Clients.builder(String.format("%s://%s:%s/", "http", LOCAL_IP, port))
                .writeTimeout(Duration.ofSeconds(3));

        return clientBuilder.build(WebClient.class);
    }

    private HttpServer createServer(final int port) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(port);
        HttpServer httpServer = null;
        httpServer = HttpServer.create(socketAddress, 0);
        return httpServer;
    }

    private static class TestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            String response = "test server started";
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream os = httpExchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

}
