/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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

    private final ResponseHandler responseHandler;
    private final PeerForwarderProvider peerForwarderProvider;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final ObjectMapper objectMapper;
    private final Timer serverRequestProcessingLatencyTimer;

    public PeerForwarderHttpService(final ResponseHandler responseHandler,
                                    final PeerForwarderProvider peerForwarderProvider,
                                    final PeerForwarderConfiguration peerForwarderConfiguration,
                                    final ObjectMapper objectMapper,
                                    final PluginMetrics pluginMetrics) {
        this.responseHandler = responseHandler;
        this.peerForwarderProvider = peerForwarderProvider;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.objectMapper = objectMapper;
        serverRequestProcessingLatencyTimer = pluginMetrics.timer(SERVER_REQUEST_PROCESSING_LATENCY);
    }

    @Post
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return serverRequestProcessingLatencyTimer.record(() -> processRequest(aggregatedHttpRequest));
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) {

        WireEvents wireEvents;
        final HttpData content = aggregatedHttpRequest.content();
        try {
            wireEvents = objectMapper.readValue(content.toStringUtf8(), WireEvents.class);
        } catch (JsonProcessingException e) {
            final String message = "Failed to write the request content due to bad request data format. Needs to be JSON object";
            LOG.error(message, e);
            return responseHandler.handleException(e, message);
        }

        try {
            writeEventsToBuffer(wireEvents);
        } catch (Exception e) {
            final String message = String.format("Failed to write the request of size %d due to:", content.length());
            LOG.error(message, e);
            return responseHandler.handleException(e, message);
        }

        return HttpResponse.of(HttpStatus.OK);
    }

    private void writeEventsToBuffer(final WireEvents wireEvents) throws Exception {
        final PeerForwarderReceiveBuffer<Record<Event>> recordPeerForwarderReceiveBuffer = getPeerForwarderBuffer(wireEvents);

        if (wireEvents.getEvents() != null) {
            final Collection<Record<Event>> jacksonEvents = wireEvents.getEvents().stream()
                    .map(this::transformEvent)
                    .collect(Collectors.toList());

            recordPeerForwarderReceiveBuffer.writeAll(jacksonEvents, peerForwarderConfiguration.getRequestTimeout());
        }
    }

    private PeerForwarderReceiveBuffer<Record<Event>> getPeerForwarderBuffer(final WireEvents wireEvents) {
        final String destinationPluginId = wireEvents.getDestinationPluginId();
        final String destinationPipelineName = wireEvents.getDestinationPipelineName();

        final Map<String, Map<String, PeerForwarderReceiveBuffer<Record<Event>>>> pipelinePeerForwarderReceiveBufferMap =
                peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap();

        return pipelinePeerForwarderReceiveBufferMap
                .get(destinationPipelineName).get(destinationPluginId);
    }

    private Record<Event> transformEvent(final WireEvent wireEvent) {
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
        return new Record<>(event);
    }

    private DefaultEventMetadata getEventMetadata(final WireEvent wireEvent) {
        return DefaultEventMetadata.builder()
                .withEventType(wireEvent.getEventType())
                .withTimeReceived(wireEvent.getEventTimeReceived())
                .withAttributes(wireEvent.getEventAttributes())
                .build();
    }
}