package com.amazon.dataprepper.plugins.processor.peerforwarder;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

@NotThreadSafe
public class HashRing {
    private static final String MD5 = "MD5";
    private final List<String> serverIps = new ArrayList<>();
    private final int numVirtualNodes;
    private final TreeMap<BigInteger, String> virtualNodes = new TreeMap<>();

    public HashRing(final List<String> serverIps, final int numVirtualNodes) {
        Objects.requireNonNull(serverIps);
        this.numVirtualNodes = numVirtualNodes;
        for (final String serverIp: serverIps) {
            addServerIp(serverIp);
        }
    }

    public List<String> getServerIps() {
        return serverIps;
    }

    private void addServerIp(final String serverIp) {
        serverIps.add(serverIp);
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
            virtualNodes.putIfAbsent(hashcode, serverIp);
            md.reset();
            intBuffer.clear();
        }
    }

    public Optional<String> getServerIp(final String traceId) {
        if (virtualNodes.isEmpty()) {
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
        final Map.Entry<BigInteger, String> entry = virtualNodes.higherEntry(hashcode);
        if (entry == null) {
            // return first node if no key is greater than the hashcode
            return Optional.of(virtualNodes.firstEntry().getValue());
        } else {
            return Optional.of(entry.getValue());
        }
    }
}
