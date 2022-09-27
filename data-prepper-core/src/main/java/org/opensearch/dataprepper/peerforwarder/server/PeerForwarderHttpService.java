/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.DefaultEventMetadata;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Timer;
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
    static final String REQUEST_PROCESSING_LATENCY = "requestProcessingLatency";

    private final ResponseHandler responseHandler;
    private final PeerForwarderProvider peerForwarderProvider;
    private final PeerForwarderConfiguration peerForwarderConfiguration;
    private final ObjectMapper objectMapper;
    private final Timer requestProcessingLatencyTimer;

    public PeerForwarderHttpService(final ResponseHandler responseHandler,
                                    final PeerForwarderProvider peerForwarderProvider,
                                    final PeerForwarderConfiguration peerForwarderConfiguration,
                                    final ObjectMapper objectMapper,
                                    final PluginMetrics pluginMetrics) {
        this.responseHandler = responseHandler;
        this.peerForwarderProvider = peerForwarderProvider;
        this.peerForwarderConfiguration = peerForwarderConfiguration;
        this.objectMapper = objectMapper;
        requestProcessingLatencyTimer = pluginMetrics.timer(REQUEST_PROCESSING_LATENCY);
    }

    @Post
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return requestProcessingLatencyTimer.record(() -> processRequest(aggregatedHttpRequest));
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
            final String message = String.format("Failed to write the request content [%s] due to:", content.toStringUtf8());
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
        final JacksonEvent jacksonEvent = JacksonEvent.builder()
                .withEventMetadata(getEventMetadata(wireEvent))
                .withData(wireEvent.getEventData())
                .build();

        return new Record<>(jacksonEvent);
    }

    private DefaultEventMetadata getEventMetadata(final WireEvent wireEvent) {
        return DefaultEventMetadata.builder()
                .withEventType(wireEvent.getEventType())
                .withTimeReceived(wireEvent.getEventTimeReceived())
                .withAttributes(wireEvent.getEventAttributes())
                .build();
    }
}