/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.log.JacksonLog;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class PeerForwarderClientTest {

    private static final String LOCAL_IP = "127.0.0.1";

    @Mock
    WebClient webClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    PeerForwarderClient createObjectUnderTest() {
        return new PeerForwarderClient(objectMapper);
    }

    @Test
    void test_serializeRecordsAndSendHttpRequest_with_mock_client_should_return() {
        when(webClient.uri()).thenReturn(URI.create("http://127.0.0.1:"));
        final HttpResponse httpResponse = mock(HttpResponse.class);
        AggregatedHttpResponse aggregatedHttpResponseMock =
                mock(AggregatedHttpResponse.class);
        when(aggregatedHttpResponseMock.status()).thenReturn(HttpStatus.OK);
        when(webClient.post(anyString(), anyString())).thenReturn(httpResponse);

        when(httpResponse.aggregate()).thenReturn(CompletableFuture.completedFuture(aggregatedHttpResponseMock));

        final PeerForwarderClient peerForwarderClient = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse =
                peerForwarderClient.serializeRecordsAndSendHttpRequest(generateBatchRecords(1), webClient);

        assertThat(aggregatedHttpResponse, notNullValue());
        assertThat(aggregatedHttpResponse, instanceOf(AggregatedHttpResponse.class));
        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
    }

    @Test
    void test_serializeRecordsAndSendHttpRequest_with_actual_client_and_server_should_return() {
        final HttpServer server = createServer(2022);
        server.createContext("/", new testHandler());
        server.start();

        final InetSocketAddress address = server.getAddress();
        final WebClient testClient = getTestClient(String.valueOf(address.getPort()));

        final PeerForwarderClient peerForwarderClient = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse =
                peerForwarderClient.serializeRecordsAndSendHttpRequest(generateBatchRecords(1), testClient);

        assertThat(aggregatedHttpResponse, notNullValue());
        assertThat(aggregatedHttpResponse, instanceOf(AggregatedHttpResponse.class));
        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
        server.stop(0);
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

    private HttpServer createServer(final int port) {
        final InetSocketAddress socketAddress = new InetSocketAddress(port);
        HttpServer httpServer = null;
        try {
            httpServer = HttpServer.create(socketAddress, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return httpServer;
    }

    private static class testHandler implements HttpHandler {
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
