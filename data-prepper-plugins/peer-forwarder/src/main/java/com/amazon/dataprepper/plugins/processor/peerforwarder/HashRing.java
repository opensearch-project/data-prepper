package com.amazon.dataprepper.plugins.processor.peerforwarder;

import com.amazon.dataprepper.plugins.processor.peerforwarder.discovery.PeerListProvider;

import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

@NotThreadSafe
public class HashRing {
    private static final String MD5 = "MD5";

    private final int numVirtualNodes;
    private final PeerListProvider peerListProvider;

    private TreeMap<BigInteger, String> hashServerMap = new TreeMap<>();

    public HashRing(final PeerListProvider peerListProvider, final int numVirtualNodes) {
        Objects.requireNonNull(peerListProvider);
        this.peerListProvider = peerListProvider;
        this.numVirtualNodes = numVirtualNodes;

        buildHashServerMap();
    }

    public Optional<String> getServerIp(final String traceId) {
        if (hashServerMap.isEmpty()) {
            return Optional.empty();
        }
        final byte[] traceIdInBytes = traceId.getBytes();
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(MD5);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.update(traceIdInBytes);
        final BigInteger hashcode = new BigInteger(md.digest());
        // obtain Map.Entry with key greater than the hashcode
        final Map.Entry<BigInteger, String> entry = hashServerMap.higherEntry(hashcode);
        if (entry == null) {
            // return first node if no key is greater than the hashcode
            return Optional.of(hashServerMap.firstEntry().getValue());
        } else {
            return Optional.of(entry.getValue());
        }
    }

    private void buildHashServerMap() {
        final TreeMap<BigInteger, String> newHashValueMap = new TreeMap<>();
        for (final String serverIp : peerListProvider.getPeerList()) {
            addServerIpToHashMap(serverIp, newHashValueMap);
        }

        this.hashServerMap = newHashValueMap;
    }

    private void addServerIpToHashMap(final String serverIp, final Map<BigInteger, String> targetMap) {
        final byte[] serverIpInBytes = serverIp.getBytes();
        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(MD5);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        final ByteBuffer intBuffer = ByteBuffer.allocate(4);
        for (int i = 0; i < numVirtualNodes; i++) {
            md.update(serverIpInBytes);
            intBuffer.putInt(i);
            md.update(intBuffer.array());
            final BigInteger hashcode = new BigInteger(md.digest());
            targetMap.putIfAbsent(hashcode, serverIp);
            md.reset();
            intBuffer.clear();
        }
    }
}
