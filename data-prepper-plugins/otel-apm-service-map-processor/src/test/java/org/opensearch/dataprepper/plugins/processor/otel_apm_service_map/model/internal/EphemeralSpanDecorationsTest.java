/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EphemeralSpanDecorationsTest {

    private EphemeralSpanDecorations decorations;
    private ClientSpanDecoration clientDecoration;
    private ServerSpanDecoration serverDecoration;

    @BeforeEach
    void setUp() {
        decorations = new EphemeralSpanDecorations();
        clientDecoration = new ClientSpanDecoration("parentOp", "env", "service", "op", Collections.emptyMap());
        serverDecoration = new ServerSpanDecoration(Collections.emptyList());
    }

    @Test
    void testClientDecorationOperations() {
        String spanId = "span123";
        
        assertNull(decorations.getClientDecoration(spanId));
        assertFalse(decorations.hasClientDecoration(spanId));
        
        decorations.setClientDecoration(spanId, clientDecoration);
        
        assertEquals(clientDecoration, decorations.getClientDecoration(spanId));
        assertTrue(decorations.hasClientDecoration(spanId));
    }

    @Test
    void testServerDecorationOperations() {
        String spanId = "span456";
        
        assertNull(decorations.getServerDecoration(spanId));
        assertFalse(decorations.hasServerDecoration(spanId));
        
        decorations.setServerDecoration(spanId, serverDecoration);
        
        assertEquals(serverDecoration, decorations.getServerDecoration(spanId));
        assertTrue(decorations.hasServerDecoration(spanId));
    }

    @Test
    void testSizeAndClear() {
        assertEquals(0, decorations.size());
        
        decorations.setClientDecoration("client1", clientDecoration);
        decorations.setServerDecoration("server1", serverDecoration);
        
        assertEquals(2, decorations.size());
        
        decorations.clear();
        
        assertEquals(0, decorations.size());
        assertFalse(decorations.hasClientDecoration("client1"));
        assertFalse(decorations.hasServerDecoration("server1"));
    }
}
