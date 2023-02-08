package org.opensearch.dataprepper.peerforwarder.codec;

public interface PeerForwarderCodec<T> {
    byte[] serialize(T events) throws Exception;

    T deserialize(byte[] bytes) throws Exception;
}
