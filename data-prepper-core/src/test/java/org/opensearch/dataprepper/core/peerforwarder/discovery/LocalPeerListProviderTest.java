/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;
import org.opensearch.dataprepper.core.peerforwarder.discovery.LocalPeerListProvider;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class LocalPeerListProviderTest {
    private final HashRing hashRing = mock(HashRing.class);

    @Test
    void testGetPeerList() {
        final LocalPeerListProvider localPeerListProvider = new LocalPeerListProvider();
        assertThat(localPeerListProvider.getPeerList(), equalTo(Collections.emptyList()));
    }

    @Test
    void testAddListener() {
        final LocalPeerListProvider localPeerListProvider = new LocalPeerListProvider();
        localPeerListProvider.addListener(hashRing);

        verifyNoInteractions(hashRing);
    }

    @Test
    void testRemoveListener() {
        final LocalPeerListProvider localPeerListProvider = new LocalPeerListProvider();
        localPeerListProvider.removeListener(hashRing);

        verifyNoInteractions(hashRing);
    }

}