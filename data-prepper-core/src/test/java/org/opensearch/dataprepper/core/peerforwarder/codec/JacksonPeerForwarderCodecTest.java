/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.codec.JacksonPeerForwarderCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;
import org.opensearch.dataprepper.core.peerforwarder.model.WireEvents;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JacksonPeerForwarderCodecTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String PLUGIN_ID = "plugin_id";
    private static final String PIPELINE_NAME = "pipeline_name";
    private final JacksonPeerForwarderCodec objectUnderTest = new JacksonPeerForwarderCodec(OBJECT_MAPPER);

    @Test
    void testSerializedBytesDeserializableIntoWireEvents() throws IOException {
        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        final byte[] bytes = objectUnderTest.serialize(inputEvents);
        final WireEvents wireEvents = OBJECT_MAPPER.readValue(bytes, WireEvents.class);
        assertThat(wireEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(wireEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(wireEvents.getEvents().size(), equalTo(2));
    }

    @Test
    void testCodec() throws IOException {
        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        final byte[] bytes = objectUnderTest.serialize(inputEvents);
        final PeerForwardingEvents outputEvents = objectUnderTest.deserialize(bytes);
        assertThat(outputEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(outputEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(outputEvents.getEvents().size(), equalTo(inputEvents.getEvents().size()));
    }

    @Test
    void testDeserializeException(){
        final byte[] bytes = new byte[0];
        assertThrows(IOException.class, () -> objectUnderTest.deserialize(bytes));
    }

    private PeerForwardingEvents generatePeerForwardingEvents(final int numEvents) {
        final List<Event> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value");
            eventData.put("key2", "value");
            final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
            events.add(event);
        }
        return new PeerForwardingEvents(events, PLUGIN_ID, PIPELINE_NAME);
    }
}