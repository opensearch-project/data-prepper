/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class XRayOTLPSinkTest {
    private XRayOTLPSink sink;

    @BeforeEach
    void setUp() {
        sink = new XRayOTLPSink();
    }

    @Test
    void testInitialize_doesNotThrow() {
        assertDoesNotThrow(() -> sink.initialize());
    }

    @Test
    void testOutput_printsRecordData() {
        Record<String> record = new Record<>("mock-otlp-span");
        assertDoesNotThrow(() -> sink.output(Collections.singletonList(record)));
    }

    @Test
    void testIsReady_returnsTrue() {
        assertTrue(sink.isReady());
    }

    @Test
    void testShutdown_doesNotThrow() {
        assertDoesNotThrow(() -> sink.shutdown());
    }
}
