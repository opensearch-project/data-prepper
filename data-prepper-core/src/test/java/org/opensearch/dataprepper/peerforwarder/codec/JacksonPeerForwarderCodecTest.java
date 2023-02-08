package org.opensearch.dataprepper.peerforwarder.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;

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
    void testCodec() throws IOException {
        final WireEvents inputEvents = generateWireEvents(2);
        final byte[] bytes = objectUnderTest.serialize(inputEvents);
        final WireEvents outputEvents = objectUnderTest.deserialize(bytes);
        assertThat(outputEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(outputEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(outputEvents.getEvents().size(), equalTo(inputEvents.getEvents().size()));
    }

    @Test
    void testDeserializeException(){
        final byte[] bytes = new byte[0];
        assertThrows(IOException.class, () -> objectUnderTest.deserialize(bytes));
    }

    private WireEvents generateWireEvents(final int numEvents) {
        final List<WireEvent> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            final Map<String, String> eventData = new HashMap<>();
            eventData.put("key1", "value");
            eventData.put("key2", "value");
            final JacksonEvent event = JacksonLog.builder().withData(eventData).withEventType("LOG").build();
            events.add(new WireEvent(
                    event.getMetadata().getEventType(),
                    event.getMetadata().getTimeReceived(),
                    event.getMetadata().getAttributes(),
                    event.toJsonString()));
        }
        return new WireEvents(events, PLUGIN_ID, PIPELINE_NAME);
    }
}