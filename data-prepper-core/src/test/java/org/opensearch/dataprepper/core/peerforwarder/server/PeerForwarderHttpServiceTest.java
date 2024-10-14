/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.core.peerforwarder.server.ResponseHandler;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
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
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.core.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.core.peerforwarder.server.PeerForwarderHttpService.RECORDS_RECEIVED_FROM_PEERS;
import static org.opensearch.dataprepper.core.peerforwarder.server.PeerForwarderHttpService.SERVER_REQUEST_PROCESSING_LATENCY;

@ExtendWith(MockitoExtension.class)
class PeerForwarderHttpServiceTest {
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";
    private static final int TEST_BUFFER_CAPACITY = 3;
    private static final int TEST_BATCH_SIZE = 3;


    private final PeerForwarderReceiveBuffer peerForwarderReceiveBuffer =
            new PeerForwarderReceiveBuffer(TEST_BATCH_SIZE, TEST_BUFFER_CAPACITY, PIPELINE_NAME, PLUGIN_ID);

    @Mock
    private PeerForwarderCodec peerForwarderCodec;

    @Mock
    private PeerForwardingEvents peerForwardingEvents;

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

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    private Timer serverRequestProcessingLatencyTimer;

    @BeforeEach
    void setUp() throws Exception {
        final List<Event> events = generateEvents(1);
        lenient().when(peerForwarderCodec.deserialize(any())).thenReturn(peerForwardingEvents);
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
                peerForwarderCodec, acknowledgementSetManager, pluginMetrics);
    }

    @Test
    void test_doPost_with_HTTP_request_should_return_OK() throws Exception {
        final HashMap<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap = new HashMap<>();
        pipelinePeerForwarderReceiveBufferMap.put(PIPELINE_NAME, Map.of(PLUGIN_ID, peerForwarderReceiveBuffer));
        when(peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap()).thenReturn(pipelinePeerForwarderReceiveBufferMap);

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.OK));

        verify(recordsReceivedFromPeersCounter).increment(1);
    }

    @Test
    void test_doPost_with_bad_HTTP_request_should_return_BAD_REQUEST() throws Exception {
        when(responseHandler.handleException(any(IOException.class), anyString())).thenReturn(HttpResponse.of(HttpStatus.BAD_REQUEST));
        lenient().when(peerForwarderCodec.deserialize(any())).thenThrow(new IOException());

        final PeerForwarderHttpService objectUnderTest = createObjectUnderTest();

        final AggregatedHttpResponse aggregatedHttpResponse = objectUnderTest.doPost(aggregatedHttpRequest).aggregate().get();

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.BAD_REQUEST));
    }

    @Test
    void test_doPost_with_HTTP_request_size_greater_than_buffer_size_should_return_REQUEST_ENTITY_TOO_LARGE()
            throws ExecutionException, InterruptedException {
        final List<Event> events = generateEvents(TEST_BUFFER_CAPACITY + 1);
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
