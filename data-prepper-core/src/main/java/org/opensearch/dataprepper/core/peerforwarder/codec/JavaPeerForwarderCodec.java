/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

public class JavaPeerForwarderCodec implements PeerForwarderCodec {
    private final ObjectInputFilter filter;

    public JavaPeerForwarderCodec(final ObjectInputFilter filter) {
        this.filter = Objects.requireNonNull(filter);
    }

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
            objectInputStream.setObjectInputFilter(filter);
            return (PeerForwardingEvents) objectInputStream.readObject();
        }
    }
}
