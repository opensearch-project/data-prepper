/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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