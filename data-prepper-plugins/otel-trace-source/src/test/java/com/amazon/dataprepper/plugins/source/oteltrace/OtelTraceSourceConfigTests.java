/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class OtelTraceSourceConfigTests {
    @Test
    public void testDefault() {

        // Prepare
        final OTelTraceSourceConfig otelTraceSourceConfig = new OTelTraceSourceConfig();


        // When/Then
        assertEquals(OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, otelTraceSourceConfig.getRequestTimeoutInMillis());
        assertEquals(OTelTraceSourceConfig.DEFAULT_PORT, otelTraceSourceConfig.getPort());
        assertEquals(OTelTraceSourceConfig.DEFAULT_THREAD_COUNT, otelTraceSourceConfig.getThreadCount());
        assertEquals(OTelTraceSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, otelTraceSourceConfig.getMaxConnectionCount());
        assertFalse(otelTraceSourceConfig.hasHealthCheck());
        assertFalse(otelTraceSourceConfig.hasProtoReflectionService());
        assertTrue(otelTraceSourceConfig.isSsl());
        assertNull(otelTraceSourceConfig.getSslKeyCertChainFile());
        assertNull(otelTraceSourceConfig.getSslKeyFile());
    }
}
