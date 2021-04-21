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

package com.amazon.dataprepper.plugins.sink.opensearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.opensearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectionConfigurationTests {
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();

    @Test
    public void testReadConnectionConfigurationDefault() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
    }

    @Test
    public void testCreateClientDefault() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testReadConnectionConfigurationNoCert() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
    }

    @Test
    public void testCreateClientNoCert() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientWithCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientWithAWSSigV4AndRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, "us-west-2", null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-west-2", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());;
    }

    @Test
    public void testCreateClientWithAWSSigV4DefaultRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());;
    }

    @Test
    public void testCreateClientWithAWSSigV4AndInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
    }

    @Test
    public void testCreateClientWithAWSSigV4AndCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());;
    }

    private PluginSetting generatePluginSetting(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String certPath, final boolean insecure) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("hosts", hosts);
        metadata.put("username", username);
        metadata.put("password", password);
        metadata.put("connect_timeout", connectTimeout);
        metadata.put("socket_timeout", socketTimeout);
        metadata.put("aws_sigv4", awsSigv4);
        if (awsRegion != null) {
            metadata.put("aws_region", awsRegion);
        }
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);

        return new PluginSetting("elasticsearch", metadata);
    }
}
