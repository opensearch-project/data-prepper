/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.server;

import com.amazon.dataprepper.model.event.DefaultEventMetadata;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Post;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerForwarderHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(PeerForwarderHttpService.class);

    private final ObjectMapper objectMapper;

    public PeerForwarderHttpService(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Post
    public HttpResponse doPost(final AggregatedHttpRequest aggregatedHttpRequest) {
        return processRequest(aggregatedHttpRequest);
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) {

        WireEvents wireEvents;
        final HttpData content = aggregatedHttpRequest.content();
        try {
            wireEvents = objectMapper.readValue(content.toStringUtf8(), WireEvents.class);
        } catch (JsonProcessingException e) {
            String message = "Failed to write the request content due to bad request data format. Needs to be JSON object";
            LOG.error(message, e);
            return HttpResponse.of(HttpStatus.BAD_REQUEST, MediaType.ANY_TYPE, message);
        }

        final String destinationPluginId = wireEvents.getDestinationPluginId();
        // TODO: send all events to corresponding buffer based on plugin id

        if (wireEvents.getEvents() != null) {
            wireEvents.getEvents().forEach(
                    wireEvent -> {
                        final Event event = getEvent(wireEvent);
                        // TODO: write event to buffer
                    }
            );
        }

        return HttpResponse.of(HttpStatus.OK);
    }

    private JacksonEvent getEvent(final WireEvent wireEvent) {
        return JacksonEvent.builder()
                .withEventMetadata(getEventMetadata(wireEvent))
                .withData(wireEvent.getEventData())
                .build();
    }

    private DefaultEventMetadata getEventMetadata(final WireEvent wireEvent) {
        return DefaultEventMetadata.builder()
                .withEventType(wireEvent.getEventType())
                .withTimeReceived(wireEvent.getEventTimeReceived())
                .withAttributes(wireEvent.getEventAttributes())
                .build();
    }
}