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

package com.amazon.dataprepper.plugins.source.loghttp;

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class HTTPSourceConfigTest {
    @Test
    public void testDefault() {
        // Prepare
        final HTTPSourceConfig sourceConfig = new HTTPSourceConfig();

        // When/Then
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HTTPSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(HTTPSourceConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
    }
}