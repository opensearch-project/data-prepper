package org.opensearch.dataprepper.peerforwarder.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.peerforwarder.model.WireEvents;

import java.io.IOException;

public class JacksonPeerForwarderCodec implements PeerForwarderCodec<WireEvents> {
    private final ObjectMapper objectMapper;

    public JacksonPeerForwarderCodec(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(final WireEvents events) throws IOException {
        return objectMapper.writeValueAsBytes(events);
    }

    @Override
    public WireEvents deserialize(final byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, WireEvents.class);
    }
}
