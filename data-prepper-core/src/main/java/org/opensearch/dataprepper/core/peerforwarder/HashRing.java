/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import com.linecorp.armeria.client.Endpoint;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * Consistent hashing implementation used to map identification keys to Data Prepper hosts.
 * See https://en.wikipedia.org/wiki/Consistent_hashing for more information.
 */
@NotThreadSafe
public class HashRing implements Consumer<List<Endpoint>> {
    private static final Logger LOG = LoggerFactory.getLogger(HashRing.class);
    private static final String MD5 = "MD5";
    private static final String DELIMITER = ",";

    /* Number of virtual nodes per Data Prepper host to be present on the hash ring */
    private final int numVirtualNodes;

    private final PeerListProvider peerListProvider;

    private TreeMap<BigInteger, String> hashServerMap = new TreeMap<>();
    private int peerCount = 0;

    public HashRing(final PeerListProvider peerListProvider, final int numVirtualNodes) {
        Objects.requireNonNull(peerListProvider);
        this.peerListProvider = peerListProvider;
        this.numVirtualNodes = numVirtualNodes;

        buildHashServerMap();

        peerListProvider.addListener(this);
    }

    public Optional<String> getServerIp(final List<String> identificationKeyValues) {
        if (hashServerMap.isEmpty()) {
            return Optional.empty();
        }

        final byte[] identificationKeysInBytes = String.join(DELIMITER, identificationKeyValues).getBytes();

        final MessageDigest md;
        try {
            md = MessageDigest.getInstance(MD5);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("unreachable", e);
        }

        md.update(identificationKeysInBytes);
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

    public int getPeerCount() {
        return peerCount;
    }

    @Override
    public void accept(final List<Endpoint> endpoints) {
        buildHashServerMap();
    }

    private void buildHashServerMap() {
        final TreeMap<BigInteger, String> newHashValueMap = new TreeMap<>();
        final List<String> endpoints = peerListProvider.getPeerList();

        LOG.info("Building hash ring with endpoints: {}", endpoints);
        for (final String serverIp : endpoints) {
            addServerIpToHashMap(serverIp, newHashValueMap);
        }

        this.hashServerMap = newHashValueMap;
        this.peerCount = endpoints.size();
    }

    private void addServerIpToHashMap(final String serverIp, final Map<BigInteger, String> targetMap) {
        final byte[] serverIpInBytes = serverIp.getBytes();
        final MessageDigest md;

        try {
            md = MessageDigest.getInstance(MD5);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("unreachable", e);
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
