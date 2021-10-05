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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class HTTPSourceConfigTest {
    private static final String PLUGIN_NAME = "http";
    private static final int TEST_PORT = 45600;
    private static final int TEST_REQUEST_TIMEOUT_MS = 777;
    private static final int TEST_THREAD_COUNT = 888;
    private static final int TEST_MAX_CONNECTION_COUNT = 999;
    private static final int TEST_MAX_PENDING_REQUESTS = 666;
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    @Test
    public void testDefault() {
        // Prepare
        final HTTPSourceConfig sourceConfig = HTTPSourceConfig.buildConfig(
                new PluginSetting(PLUGIN_NAME, new HashMap<>()));

        // When/Then
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HTTPSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(HTTPSourceConfig.DEFAULT_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(HTTPSourceConfig.DEFAULT_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
    }

    @Test
    public void testValidConfigSSLDisabled() {
        // Prepare
        final PluginSetting pluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS,
                false,
                null,
                null,
                null
        );
        final HTTPSourceConfig sourceConfig = HTTPSourceConfig.buildConfig(pluginSetting);

        // When/Then
        assertEquals(TEST_PORT, sourceConfig.getPort());
        assertEquals(TEST_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(TEST_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
        assertFalse(sourceConfig.isSsl());
        assertNull(sourceConfig.getSslCertificateFile());
        assertNull(sourceConfig.getSslKeyFile());
        assertNull(sourceConfig.getSslKeyPassword());
    }

    @Test
    public void testValidConfigSSLEnabled() {
        // Prepare
        final PluginSetting pluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS,
                true,
                TEST_SSL_CERTIFICATE_FILE,
                TEST_SSL_KEY_FILE,
                null
        );
        final HTTPSourceConfig sourceConfig = HTTPSourceConfig.buildConfig(pluginSetting);

        // When/Then
        assertEquals(TEST_PORT, sourceConfig.getPort());
        assertEquals(TEST_REQUEST_TIMEOUT_MS, sourceConfig.getRequestTimeoutInMillis());
        assertEquals(TEST_THREAD_COUNT, sourceConfig.getThreadCount());
        assertEquals(TEST_MAX_CONNECTION_COUNT, sourceConfig.getMaxConnectionCount());
        assertEquals(TEST_MAX_PENDING_REQUESTS, sourceConfig.getMaxPendingRequests());
        assertTrue(sourceConfig.isSsl());
        assertEquals(TEST_SSL_CERTIFICATE_FILE, sourceConfig.getSslCertificateFile());
        assertEquals(TEST_SSL_KEY_FILE, sourceConfig.getSslKeyFile());
        assertNull(sourceConfig.getSslKeyPassword());
    }

    @Test
    public void testInvalidPort() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                65536,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS,
                false,
                null,
                null,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidRequestTimeout() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                -1,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS,
                false,
                null,
                null,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidThreadCount() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                0,
                TEST_MAX_CONNECTION_COUNT,
                TEST_MAX_PENDING_REQUESTS,
                false,
                null,
                null,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidMaxConnectionCount() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                0,
                TEST_MAX_PENDING_REQUESTS,
                false,
                null,
                null,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidMaxPendingRequests() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                0,
                false,
                null,
                null,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidSslCert() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                0,
                true,
                "invalid path",
                TEST_SSL_KEY_FILE,
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    @Test
    public void testInvalidSslKey() {
        final PluginSetting invalidPluginSetting = completePluginSettingForLogHTTPSource(
                TEST_PORT,
                TEST_REQUEST_TIMEOUT_MS,
                TEST_THREAD_COUNT,
                TEST_MAX_CONNECTION_COUNT,
                0,
                true,
                TEST_SSL_CERTIFICATE_FILE,
                "invalid path",
                null
        );
        assertThrows(IllegalArgumentException.class, () -> HTTPSourceConfig.buildConfig(invalidPluginSetting));
    }

    private PluginSetting completePluginSettingForLogHTTPSource(final int port,
                                                                final int requestTimeoutInMillis,
                                                                final int threadCount,
                                                                final int maxConnectionCount,
                                                                final int maxPendingRequests,
                                                                final boolean ssl,
                                                                final String sslCertificateFile,
                                                                final String sslKeyFile,
                                                                final String sslKeyPassword) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(HTTPSourceConfig.PORT, port);
        settings.put(HTTPSourceConfig.REQUEST_TIMEOUT, requestTimeoutInMillis);
        settings.put(HTTPSourceConfig.THREAD_COUNT, threadCount);
        settings.put(HTTPSourceConfig.MAX_CONNECTION_COUNT, maxConnectionCount);
        settings.put(HTTPSourceConfig.MAX_PENDING_REQUESTS, maxPendingRequests);
        settings.put(HTTPSourceConfig.SSL, ssl);
        settings.put(HTTPSourceConfig.SSL_CERTIFICATE_FILE, sslCertificateFile);
        settings.put(HTTPSourceConfig.SSL_KEY_FILE, sslKeyFile);
        settings.put(HTTPSourceConfig.SSL_KEY_PASSWORD, sslKeyPassword);
        return new PluginSetting(PLUGIN_NAME, settings);
    }
}