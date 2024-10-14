/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.server;

import org.junit.jupiter.api.Test;

class NoOpPeerForwarderServerTest {

    private NoOpPeerForwarderServer createObjectUnderTest() {
        return new NoOpPeerForwarderServer();
    }

    @Test
    void test_start() {
        final NoOpPeerForwarderServer objectUnderTest = createObjectUnderTest();
        objectUnderTest.start();
    }

    @Test
    void test_stop() {
        final NoOpPeerForwarderServer objectUnderTest = createObjectUnderTest();
        objectUnderTest.stop();
    }

}