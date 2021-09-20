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

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LogHTTPSourceConfigTest {
    private static final String PLUGIN_NAME = "log_http_source";
    private static final int TEST_PORT = 45600;
    private static final int TEST_REQUEST_TIMEOUT_MS = 777;
    private static final int TEST_THREAD_COUNT = 888;
    private static final int TEST_MAX_CONNECTION_COUNT = 999;
    private static final int TEST_MAX_PENDING_REQUESTS = 666;

    @Test
    public void testDefault() {
        // Prepare
        final LogHTTPSourceConfig logHTTPSourceConfig = LogHTTPSourceConfig.buildConfig(
                new PluginSetting(PLUGIN_NAME, new HashMap<>()));

        // When/Then
        assertEquals(LogHTTPSourceConfig.DEFAULT_PORT, logHTTPSourceConfig.getPort());
        assertEquals(LogHTTPSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, logHTTPSourceConfig.getRequestTimeoutInMillis());
        assertEquals(LogHTTPSourceConfig.DEFAULT_THREAD_COUNT, logHTTPSourceConfig.getThreadCount());
        assertEquals(LogHTTPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, logHTTPSourceConfig.getMaxConnectionCount());
        assertEquals(LogHTTPSourceConfig.DEFAULT_MAX_PENDING_REQUESTS, logHTTPSourceConfig.getMaxPendingRequests());
    }

    @Test
    public void testValidConfig() {
        // TODO: write test logic
    }

    private PluginSetting completePluginSettingForLogHTTPSource(final int port,
                                                                final int requestTimeoutInMillis,
                                                                final int threadCount,
                                                                final int maxConnectionCount,
                                                                final int maxPendingRequests) {
        final Map<String, Object> settings = new HashMap<>();
        // TODO: add parameters on tls/ssl
        settings.put(LogHTTPSourceConfig.PORT, port);
        settings.put(LogHTTPSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(LogHTTPSourceConfig.THREAD_COUNT, threadCount);
        settings.put(LogHTTPSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(LogHTTPSourceConfig.MAX_PENDING_REQUESTS, maxPendingRequests);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}