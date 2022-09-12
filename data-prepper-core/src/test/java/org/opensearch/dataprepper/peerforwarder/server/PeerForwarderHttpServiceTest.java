/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration.DEFAULT_PEER_FORWARDING_URI;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServiceTest {
    private static final String MESSAGE_KEY = "key";
    private static final String MESSAGE = "message";
    private static final String LOG = "LOG";
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";

    private final ResponseHandler responseHandler = new ResponseHandler();
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private PeerForwarderReceiveBuffer peerForwarderReceiveBuffer;

    private PeerForwarderHttpService createObjectUnderTest() {
        return new PeerForwarderHttpService(responseHandler, peerForwarderProvider, peerForwarderConfiguration, objectMapper);
    }


    @Test
    void test_doPost_with_HTTP_request_should_return_OK() throws ExecutionException, JsonProcessingException, InterruptedException {
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<?>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final AggregatedHttpRequest aggregatedHttpRequest = generateRandomValidHTTPRequest();

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));
    }

    @Test
    void test_doPost_with_bad_HTTP_request_should_return_BAD_REQUEST() throws ExecutionException, InterruptedException {
        final AggregatedHttpRequest aggregatedHttpRequest = generateBadHTTPRequest();

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
    }

    private AggregatedHttpRequest generateRandomValidHTTPRequest() throws JsonProcessingException,
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

        final WireEvent wireEvent = new WireEvent(event.getMetadata().getEventType(),
                event.getMetadata().getTimeReceived(),
                event.getMetadata().getAttributes(),
                event.toJsonString());

        final WireEvents wireEvents = new WireEvents(List.of(wireEvent), PLUGIN_ID, PIPELINE_NAME);

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

}
