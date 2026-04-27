/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.discovery;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.peerforwarder.HashRing;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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