/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.peerforwarder.PeerForwarderReceiveBuffer;
import org.opensearch.dataprepper.peerforwarder.codec.JacksonPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.JavaPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An annotated HTTP service class to handle POST requests used by {@link PeerForwarderHttpServerProvider}
 *
 * @since 2.0
 */
public class PeerForwarderHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderHttpService.class);
    private static final String TRACE_EVENT_TYPE = "TRACE";
    static final String SERVER_REQUEST_PROCESSING_LATENCY = "serverRequestProcessingLatency";
    static final String RECORDS_RECEIVED_FROM_PEERS = "recordsReceivedFromPeers";
    private static final double BUFFER_TIMEOUT_FRACTION = 0.8;

    private final ResponseHandler responseHandler;
    private final PeerForwarderProvider peerForwarderProvider;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final JavaPeerForwarderCodec javaPeerForwarderCodec;
    private final JacksonPeerForwarderCodec jacksonPeerForwarderCodec;
    private final Timer serverRequestProcessingLatencyTimer;
    private final Counter recordsReceivedFromPeersCounter;

    public PeerForwarderHttpService(final ResponseHandler responseHandler,
                                    final PeerForwarderProvider peerForwarderProvider,
                                    final PeerForwarderConfiguration peerForwarderConfiguration,
                                    final JavaPeerForwarderCodec javaPeerForwarderCodec,
                                    final JacksonPeerForwarderCodec jacksonPeerForwarderCodec,
                                    final PluginMetrics pluginMetrics) {
        this.responseHandler = responseHandler;
        this.peerForwarderProvider = peerForwarderProvider;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.javaPeerForwarderCodec = javaPeerForwarderCodec;
        this.jacksonPeerForwarderCodec = jacksonPeerForwarderCodec;
        serverRequestProcessingLatencyTimer = pluginMetrics.timer(SERVER_REQUEST_PROCESSING_LATENCY);
        recordsReceivedFromPeersCounter = pluginMetrics.counter(RECORDS_RECEIVED_FROM_PEERS);
    }

    @Post
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return serverRequestProcessingLatencyTimer.record(() -> processRequest(aggregatedHttpRequest));
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) {

        PeerForwardingEvents peerForwardingEvents;
        final HttpData content = aggregatedHttpRequest.content();
        final List<Event> events = new ArrayList<>();
        final String destinationPluginId;
        final String destinationPipelineName;
        if (peerForwarderConfiguration.getBinaryCodec()) {
            try {
                peerForwardingEvents = javaPeerForwarderCodec.deserialize(content.array());
                destinationPluginId = peerForwardingEvents.getDestinationPluginId();
                destinationPipelineName = peerForwardingEvents.getDestinationPipelineName();
                if (peerForwardingEvents.getEvents() != null) {
                    events.addAll(peerForwardingEvents.getEvents());
                }
            } catch (IOException | ClassNotFoundException e) {
                final String message = "Failed to write the request content due to bad request data format. Needs to be JSON object";
                LOG.error(message, e);
                return responseHandler.handleException(e, message);
            }
        } else {
            try {
                final WireEvents wireEvents = jacksonPeerForwarderCodec.deserialize(content.array());
                destinationPluginId = wireEvents.getDestinationPluginId();
                destinationPipelineName = wireEvents.getDestinationPipelineName();
                if (wireEvents.getEvents() != null) {
                    events.addAll(wireEvents.getEvents().stream().map(this::transformEvent)
                            .collect(Collectors.toList()));
                }
            } catch (IOException e) {
                final String message = "Failed to write the request content due to bad request data format. Needs to be JSON object";
                LOG.error(message, e);
                return responseHandler.handleException(e, message);
            }
        }

        try {
            writeEventsToBuffer(events, destinationPluginId, destinationPipelineName);
        } catch (Exception e) {
            final String message = String.format("Failed to write the request of size %d due to:", content.length());
            LOG.error(message, e);
            return responseHandler.handleException(e, message);
        }

        return HttpResponse.of(HttpStatus.OK);
    }

    private void writeEventsToBuffer(final Collection<Event> events,
                                     final String destinationPluginId,
                                     final String destinationPipelineName) throws Exception {
        final PeerForwarderReceiveBuffer<Record<Event>> recordPeerForwarderReceiveBuffer = getPeerForwarderBuffer(
                destinationPluginId, destinationPipelineName);

        final Collection<Record<Event>> jacksonEvents = events.stream().map(Record::new)
                .collect(Collectors.toList());

        recordPeerForwarderReceiveBuffer.writeAll(jacksonEvents, getBufferTimeoutMillis());
        recordsReceivedFromPeersCounter.increment(jacksonEvents.size());
    }

    private int getBufferTimeoutMillis() {
        return (int) (peerForwarderConfiguration.getRequestTimeout() * BUFFER_TIMEOUT_FRACTION);
    }

    private PeerForwarderReceiveBuffer<Record<Event>> getPeerForwarderBuffer(final String destinationPluginId,
                                                                             final String destinationPipelineName) {
        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap =
                peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap();

        return pipelinePeerForwarderReceiveBufferMap
                .get(destinationPipelineName).get(destinationPluginId);
    }

    private Event transformEvent(final WireEvent wireEvent) {
        final DefaultEventMetadata eventMetadata = getEventMetadata(wireEvent);
        Event event;

        if (wireEvent.getEventType().equalsIgnoreCase(TRACE_EVENT_TYPE)) {
            event = JacksonSpan.builder()
                    .withJsonData(wireEvent.getEventData())
                    .withEventMetadata(eventMetadata)
                    .build();
        } else {
            event = JacksonEvent.builder()
                    .withData(wireEvent.getEventData())
                    .withEventMetadata(eventMetadata)
                    .build();
        }
        return event;
    }

    private DefaultEventMetadata getEventMetadata(final WireEvent wireEvent) {
        return DefaultEventMetadata.builder()
                .withEventType(wireEvent.getEventType())
                .withTimeReceived(wireEvent.getEventTimeReceived())
                .withAttributes(wireEvent.getEventAttributes())
                .build();
    }
}