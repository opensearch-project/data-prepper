package org.opensearch.dataprepper.peerforwarder.codec;

import org.opensearch.dataprepper.peerforwarder.model.PeerForwardingEvents;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaPeerForwarderCodec implements PeerForwarderCodec<PeerForwardingEvents> {

    @Override
    public byte[] serialize(final PeerForwardingEvents events) throws IOException {
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(events);
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public PeerForwardingEvents deserialize(final byte[] bytes) throws IOException, ClassNotFoundException {
        try (final InputStream inputStream = new ByteArrayInputStream(bytes);
             final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {
            return (PeerForwardingEvents) objectInputStream.readObject();
        }
    }
}
