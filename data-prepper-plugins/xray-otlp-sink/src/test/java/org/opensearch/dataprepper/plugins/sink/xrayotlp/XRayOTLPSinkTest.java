/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

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
        final Span mockSpan = mock(Span.class);
        final Record<Span> record = new Record<>(mockSpan);
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
