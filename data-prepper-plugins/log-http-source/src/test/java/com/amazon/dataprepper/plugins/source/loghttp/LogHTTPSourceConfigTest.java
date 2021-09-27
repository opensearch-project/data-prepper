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
import static org.junit.Assert.assertThrows;

public class LogHTTPSourceConfigTest {
    private static final String PLUGIN_NAME = "log_http";
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
        // Prepare
        final PluginSetting pluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS
        );
        final LogHTTPSourceConfig logHTTPSourceConfig = LogHTTPSourceConfig.buildConfig(pluginSetting);

        // When/Then
        assertEquals(TEST_PORT, logHTTPSourceConfig.getPort());
        assertEquals(TEST_REQUEST_TIMEOUT_MS, logHTTPSourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_THREAD_COUNT, logHTTPSourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, logHTTPSourceConfig.getMaxConnectionCount());
        assertEquals(TEST_MAX_PENDING_REQUESTS, logHTTPSourceConfig.getMaxPendingRequests());
    }

    @Test
    public void testInvalidConfig() {
        final PluginSetting invalidPluginSetting1 = completePluginSettingForLogHTTPSource(
                65536,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS
        );
        assertThrows(IllegalArgumentException.class, () -> LogHTTPSourceConfig.buildConfig(invalidPluginSetting1));

        // Invalid request_timeout
        final PluginSetting invalidPluginSetting2 = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                -1,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS
        );
        assertThrows(IllegalArgumentException.class, () -> LogHTTPSourceConfig.buildConfig(invalidPluginSetting2));

        // Invalid thread_count
        final PluginSetting invalidPluginSetting3 = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                0,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS
        );
        assertThrows(IllegalArgumentException.class, () -> LogHTTPSourceConfig.buildConfig(invalidPluginSetting3));

        // Invalid max_connection_count
        final PluginSetting invalidPluginSetting4 = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                0,
                TEST_MAX_PENDING_REQUESTS
        );
        assertThrows(IllegalArgumentException.class, () -> LogHTTPSourceConfig.buildConfig(invalidPluginSetting4));

        // Invalid max_pending_requests
        final PluginSetting invalidPluginSetting5 = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                0
        );
        assertThrows(IllegalArgumentException.class, () -> LogHTTPSourceConfig.buildConfig(invalidPluginSetting5));
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