/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.noop.NoopTimer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.peerforwarder.codec.JacksonPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.JavaPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService.RECORDS_RECEIVED_FROM_PEERS;
import static org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService.SERVER_REQUEST_PROCESSING_LATENCY;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServiceTest {
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_BATCH_SIZE = 3;


    private final PeerForwarderReceiveBuffer peerForwarderReceiveBuffer =
            new PeerForwarderReceiveBuffer(TEST_BATCH_SIZE, TEST_BUFFER_CAPACITY);

    @Mock
    private JavaPeerForwarderCodec javaPeerForwarderCodec;

    @Mock
    private PeerForwardingEvents peerForwardingEvents;

    @Mock
    private JacksonPeerForwarderCodec jacksonPeerForwarderCodec;

    @Mock
    private WireEvents wireEvents;

    @Mock
    private AggregatedHttpRequest aggregatedHttpRequest;

    @Mock
    private HttpData httpData;

    @Mock
    private PeerForwarderProvider peerForwarderProvider;

    @Mock
    private PeerForwarderConfiguration peerForwarderConfiguration;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ResponseHandler responseHandler;

    @Mock
    private Counter recordsReceivedFromPeersCounter;

    private Timer serverRequestProcessingLatencyTimer;

    @BeforeEach
    void setUp() throws IOException, ClassNotFoundException {
        final List<Event> events = generateEvents(1);
        lenient().when(jacksonPeerForwarderCodec.deserialize(any())).thenReturn(wireEvents);
        lenient().when(wireEvents.getEvents()).thenReturn(
                events.stream().map(event1 -> new WireEvent(
                        event1.getMetadata().getEventType(),
                        event1.getMetadata().getTimeReceived(),
                        event1.getMetadata().getAttributes(),
                        event1.toJsonString()
                        )).collect(Collectors.toList()));
        lenient().when(wireEvents.getDestinationPluginId()).thenReturn(PLUGIN_ID);
        lenient().when(wireEvents.getDestinationPipelineName()).thenReturn(PIPELINE_NAME);
        lenient().when(javaPeerForwarderCodec.deserialize(any())).thenReturn(peerForwardingEvents);
        lenient().when(peerForwardingEvents.getEvents()).thenReturn(events);
        lenient().when(peerForwardingEvents.getDestinationPluginId()).thenReturn(PLUGIN_ID);
        lenient().when(peerForwardingEvents.getDestinationPipelineName()).thenReturn(PIPELINE_NAME);
        when(aggregatedHttpRequest.content()).thenReturn(httpData);
        when(httpData.array()).thenReturn(new byte[10]);
        serverRequestProcessingLatencyTimer = new NoopTimer(new Meter.Id("test", Tags.empty(), null, null, Meter.Type.TIMER));
        when(pluginMetrics.timer(SERVER_REQUEST_PROCESSING_LATENCY)).thenReturn(serverRequestProcessingLatencyTimer);
        when(pluginMetrics.counter(RECORDS_RECEIVED_FROM_PEERS)).thenReturn(recordsReceivedFromPeersCounter);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(recordsReceivedFromPeersCounter);
    }

    private PeerForwarderHttpService createObjectUnderTest() {
        return new PeerForwarderHttpService(responseHandler, peerForwarderProvider, peerForwarderConfiguration,
                javaPeerForwarderCodec, jacksonPeerForwarderCodec, pluginMetrics);
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void test_doPost_with_HTTP_request_should_return_OK(final boolean binaryCodec) throws Exception {
        when(peerForwarderConfiguration.getBinaryCodec()).thenReturn(binaryCodec);
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));

        verify(recordsReceivedFromPeersCounter).increment(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void test_doPost_with_bad_HTTP_request_should_return_BAD_REQUEST(final boolean binaryCodec)
            throws ExecutionException, InterruptedException, IOException, ClassNotFoundException {
        when(peerForwarderConfiguration.getBinaryCodec()).thenReturn(binaryCodec);
        when(responseHandler.handleException(any(IOException.class), anyString())).thenReturn(HttpResponse.of(HttpStatus.BAD_REQUEST));
        lenient().when(jacksonPeerForwarderCodec.deserialize(any())).thenThrow(new IOException());
        lenient().when(javaPeerForwarderCodec.deserialize(any())).thenThrow(new IOException());

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void test_doPost_with_HTTP_request_size_greater_than_buffer_size_should_return_REQUEST_ENTITY_TOO_LARGE(
            final boolean binaryCodec) throws ExecutionException, JsonProcessingException, InterruptedException {
        when(peerForwarderConfiguration.getBinaryCodec()).thenReturn(binaryCodec);
        final List<Event> events = generateEvents(TEST_BUFFER_CAPACITY + 1);
        lenient().when(wireEvents.getEvents()).thenReturn(
                events.stream().map(event1 -> new WireEvent(
                        event1.getMetadata().getEventType(),
                        event1.getMetadata().getTimeReceived(),
                        event1.getMetadata().getAttributes(),
                        event1.toJsonString()
                )).collect(Collectors.toList()));
        lenient().when(peerForwardingEvents.getEvents()).thenReturn(events);
        when(responseHandler.handleException(any(SizeOverflowException.class), anyString())).thenReturn(HttpResponse.of(HttpStatus.REQUEST_ENTITY_TOO_LARGE));
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    private List<Event> generateEvents(final int numEvents) {
        final List<Event> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value");
            eventData.put("key2", "value");
            final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
            events.add(event);
        }
        return events;
    }
}
