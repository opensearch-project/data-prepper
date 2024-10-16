/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;
import org.opensearch.dataprepper.core.peerforwarder.discovery.PeerListProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
class HashRingTest {
    private static final List<String> SERVER_IPS = Arrays.asList(
            "10.10.0.1",
            "10.10.0.2",
            "10.10.0.3");

    private static final List<String> IDENTIFICATION_KEY_LIST_1 = List.of("key1");
    private static final List<String> IDENTIFICATION_KEY_LIST_2 = List.of("key_2");

    private static final int SINGLE_VIRTUAL_NODE_COUNT = 1;
    private static final int MULTIPLE_VIRTUAL_NODE_COUNT = 100;

    private final PeerListProvider peerListProvider = mock(PeerListProvider.class);

    private HashRing hashRing;

    @BeforeEach
    public void setUp() {
        when(peerListProvider.getPeerList()).thenReturn(SERVER_IPS);
    }

    @Test
    void testGetServerIpEmptyMap() {
        when(peerListProvider.getPeerList()).thenReturn(Collections.emptyList());
        hashRing = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);

        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testGetServerIpSingleNodeSameIdentificationKeys() {
        hashRing = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);
        Optional<String> result2 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);

        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertEquals(result1.get(), result2.get());
    }

    @Test
    void testGetServerIpSingleNodeDifferentIdentificationKeys() {
        hashRing = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);
        Optional<String> result2 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_2);

        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());

        Assertions.assertNotEquals(result1.get(), result2.get());
    }

    @Test
    void testGetServerIpMultipleNodesSameIdentificationKeys() {
        hashRing = new HashRing(peerListProvider, MULTIPLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);
        Optional<String> result2 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);

        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertEquals(result1.get(), result2.get());
    }

    @Test
    void testGetServerIpMultipleDifferentIdentificationKeys() {
        hashRing = new HashRing(peerListProvider, MULTIPLE_VIRTUAL_NODE_COUNT);

        Optional<String> result1 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);
        Optional<String> result2 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_2);

        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertNotEquals(result1.get(), result2.get());
    }

    @Test
    void testSpecialCaseNoKeyInMapGreaterThanHashValue() {
        when(peerListProvider.getPeerList()).thenReturn(Collections.singletonList("serverIp"));

        hashRing = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        // IDENTIFICATION KEY SET 1 hash is less than the hash of "serverIp"
        Optional<String> result1 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_1);

        // IDENTIFICATION KEY SET 2 hash is greater than the hash of "serverIp"
        Optional<String> result2 = hashRing.getServerIp(IDENTIFICATION_KEY_LIST_2);

        // As there is only one value in the server list, the results should be the same
        Assertions.assertTrue(result1.isPresent());
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertEquals(result1.get(), result2.get());
    }

    @Test
    void testEndpointChangeRebuildsMap() {
        hashRing = new HashRing(peerListProvider, SINGLE_VIRTUAL_NODE_COUNT);

        // First call during construction
        verify(peerListProvider, times(1)).getPeerList();

        hashRing.accept(Collections.emptyList());

        // Second call during rebuild
        verify(peerListProvider, times(2)).getPeerList();
    }
}