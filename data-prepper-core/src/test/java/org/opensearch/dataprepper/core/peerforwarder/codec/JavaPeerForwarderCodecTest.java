/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.codec.JavaPeerForwarderCodec;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JavaPeerForwarderCodecTest {
    private String pipelineName;
    private String pluginId;
    private ObjectInputFilter objectInputFilter;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        pluginId = UUID.randomUUID().toString();

        objectInputFilter = mock(ObjectInputFilter.class);
        when(objectInputFilter.checkInput(any(ObjectInputFilter.FilterInfo.class))).thenReturn(ObjectInputFilter.Status.ALLOWED);
    }

    private JavaPeerForwarderCodec createObjectUnderTest() {
        return new JavaPeerForwarderCodec(objectInputFilter);
    }

    @Test
    void constructor_with_null_ObjectInputFilter_throws_exception() {
        objectInputFilter = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void testCodec() throws IOException, ClassNotFoundException {
        when(objectInputFilter.checkInput(any(ObjectInputFilter.FilterInfo.class))).thenReturn(ObjectInputFilter.Status.ALLOWED);

        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        final byte[] bytes = createObjectUnderTest().serialize(inputEvents);
        final PeerForwardingEvents outputEvents = createObjectUnderTest().deserialize(bytes);
        assertThat(outputEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(outputEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(outputEvents.getEvents().size(), equalTo(inputEvents.getEvents().size()));

        verify(objectInputFilter, atLeast(1)).checkInput(any(ObjectInputFilter.FilterInfo.class));
    }

    @Test
    void testCodec_with_acknowledgementSet() throws IOException, ClassNotFoundException {
        when(objectInputFilter.checkInput(any(ObjectInputFilter.FilterInfo.class))).thenReturn(ObjectInputFilter.Status.ALLOWED);

        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        inputEvents.getEvents().stream()
                .map(Event::getEventHandle)
                .map(handle -> (InternalEventHandle)handle)
                .forEach(handle -> handle.addAcknowledgementSet(mock(AcknowledgementSet.class)));
        final byte[] bytes = createObjectUnderTest().serialize(inputEvents);
        final PeerForwardingEvents outputEvents = createObjectUnderTest().deserialize(bytes);
        assertThat(outputEvents.getDestinationPipelineName(), equalTo(inputEvents.getDestinationPipelineName()));
        assertThat(outputEvents.getDestinationPluginId(), equalTo(inputEvents.getDestinationPluginId()));
        assertThat(outputEvents.getEvents().size(), equalTo(inputEvents.getEvents().size()));

        verify(objectInputFilter, atLeast(1)).checkInput(any(ObjectInputFilter.FilterInfo.class));
    }

    @Test
    void testDeserializeException(){
        final byte[] bytes = new byte[0];
        final JavaPeerForwarderCodec objectUnderTest = createObjectUnderTest();
        assertThrows(IOException.class, () -> objectUnderTest.deserialize(bytes));
    }

    @Test
    void deserialize_throws_when_input_is_rejected() throws IOException {
        when(objectInputFilter.checkInput(any(ObjectInputFilter.FilterInfo.class))).thenReturn(ObjectInputFilter.Status.REJECTED);

        final PeerForwardingEvents inputEvents = generatePeerForwardingEvents(2);
        final byte[] bytes = createObjectUnderTest().serialize(inputEvents);

        final JavaPeerForwarderCodec objectUnderTest = createObjectUnderTest();
        assertThrows(InvalidClassException.class, () -> objectUnderTest.deserialize(bytes));

        verify(objectInputFilter).checkInput(any(ObjectInputFilter.FilterInfo.class));
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
        return new PeerForwardingEvents(events, pluginId, pipelineName);
    }
}
