/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;
import static org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService.SERVER_REQUEST_PROCESSING_LATENCY;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServiceTest {
    private static final String MESSAGE_KEY = "key";
    private static final String MESSAGE = "message";
    private static final String LOG = "LOG";
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_BATCH_SIZE = 3;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private final PeerForwarderReceiveBuffer peerForwarderReceiveBuffer =
            new PeerForwarderReceiveBuffer(TEST_BATCH_SIZE, TEST_BUFFER_CAPACITY);

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ResponseHandler responseHandler;
    private Timer serverRequestProcessingLatencyTimer;

    @BeforeEach
    void setUp() {
        serverRequestProcessingLatencyTimer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        when(pluginMetrics.timer(SERVER_REQUEST_PROCESSING_LATENCY)).thenReturn(serverRequestProcessingLatencyTimer);
    }

    private PeerForwarderHttpService createObjectUnderTest() {
        return new PeerForwarderHttpService(responseHandler, peerForwarderProvider, peerForwarderConfiguration, objectMapper, pluginMetrics);
    }


    @Test
    void test_doPost_with_HTTP_request_should_return_OK() throws Exception {
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final AggregatedHttpRequest aggregatedHttpRequest = generateRandomValidHTTPRequest(1);

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
    }

    @Test
    void test_doPost_with_bad_HTTP_request_should_return_BAD_REQUEST() throws ExecutionException, InterruptedException {
        when(responseHandler.handleException(any(JsonProcessingException.class), anyString())).thenReturn(HttpResponse.of(HttpStatus.BAD_REQUEST));
        final AggregatedHttpRequest aggregatedHttpRequest = generateBadHTTPRequest();

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void test_doPost_with_HTTP_request_size_greater_than_buffer_size_should_return_REQUEST_ENTITY_TOO_LARGE() throws ExecutionException, JsonProcessingException, InterruptedException {
        when(responseHandler.handleException(any(SizeOverflowException.class), anyString())).thenReturn(HttpResponse.of(HttpStatus.REQUEST_ENTITY_TOO_LARGE));
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final AggregatedHttpRequest aggregatedHttpRequest = generateRandomValidHTTPRequest(TEST_BUFFER_CAPACITY + 1);
        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    private AggregatedHttpRequest generateRandomValidHTTPRequest(final int numRecords) throws JsonProcessingException,
            ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path(DEFAULT_PEER_FORWARDING_URI)
                .build();

        final JacksonEvent event = JacksonEvent.builder()
                .withTimeReceived(Instant.now())
                .withData(Collections.singletonMap(MESSAGE_KEY, MESSAGE))
                .withEventType(LOG)
                .build();

        final List<Record<Event>> records = generateBatchRecords(numRecords);

        final List<WireEvent> wireEventList = records.stream().map(record -> {
            final WireEvent wireEvent = new WireEvent(record.getData().getMetadata().getEventType(),
                event.getMetadata().getTimeReceived(),
                event.getMetadata().getAttributes(),
                event.toJsonString());

            return wireEvent;
        }).collect(Collectors.toList());

        final WireEvents wireEvents = new WireEvents(wireEventList, PLUGIN_ID, PIPELINE_NAME);

        String content = objectMapper.writeValueAsString(wireEvents);
        HttpData httpData = HttpData.ofUtf8(content);
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private AggregatedHttpRequest generateBadHTTPRequest() throws ExecutionException, InterruptedException {
        RequestHeaders requestHeaders = RequestHeaders.builder()
                .contentType(MediaType.JSON)
                .method(HttpMethod.POST)
                .path(DEFAULT_PEER_FORWARDING_URI)
                .build();
        HttpData httpData = HttpData.ofUtf8("{");
        return HttpRequest.of(requestHeaders, httpData).aggregate().get();
    }

    private List<Record<Event>> generateBatchRecords(final int numRecords) {
        final List<Record<Event>> results = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value");
            eventData.put("key2", "value");
            final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
            results.add(new Record<>(event));
        }
        return results;
    }

}
