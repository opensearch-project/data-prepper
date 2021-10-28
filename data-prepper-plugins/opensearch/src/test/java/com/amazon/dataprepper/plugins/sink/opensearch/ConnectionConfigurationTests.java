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
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ConnectionConfigurationTests {
    private static final String PROXY_PARAMETER = "proxy";
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final String TEST_PIPELINE_NAME = "Test-Pipeline";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("test-ca.pem")).getFile();

    @Test
    public void testReadConnectionConfigurationDefault() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClientDefault() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testReadConnectionConfigurationNoCert() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
        assertFalse(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClientNoCert() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientWithCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientWithAWSSigV4AndRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, "us-west-2", null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-west-2", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());;
    }

    @Test
    public void testCreateClientWithAWSSigV4DefaultRegion() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClientWithAWSSigV4AndInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClientWithAWSSigV4AndCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, null, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClientWithAWSSigV4AndSTSRole() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, true, null, "arn:aws:iam::123456789012:iam-role", TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals("us-east-1", connectionConfiguration.getAwsRegion());
        assertTrue(connectionConfiguration.isAwsSigv4());
        assertEquals("arn:aws:iam::123456789012:iam-role", connectionConfiguration.getAwsStsRoleArn());
        assertEquals(TEST_PIPELINE_NAME, connectionConfiguration.getPipelineName());
    }

    @Test
    public void testCreateClient_WithValidHttpProxy_HostIP() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "121.121.121.121:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClient_WithValidHttpProxy_HostName() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:80";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClient_WithValidHttpProxy_SchemeProvided() throws IOException {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "http://example.com:4350";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClient_WithInvalidHttpProxy_InvalidPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    public void testCreateClient_WithInvalidHttpProxy_NoPort() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    public void testCreateClient_WithInvalidHttpProxy_PortNotInRange() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "example.com:888888";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    public void testCreateClient_WithInvalidHttpProxy_NotHttp() {
        final Map<String, Object> metadata = generateConfigurationMetadata(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, false, null, null, TEST_CERT_PATH, false);
        final String testHttpProxy = "socket://example.com:port";
        metadata.put(PROXY_PARAMETER, testHttpProxy);
        final PluginSetting pluginSetting = getPluginSettingByConfigurationMetadata(metadata);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(connectionConfiguration.getProxy().get(), testHttpProxy);
        assertThrows(IllegalArgumentException.class, () -> connectionConfiguration.createClient());
    }

    @Test
    public void testCreateClient_WithConnectionConfigurationBuilder_ProxyOptionalObjectShouldNotBeNull() throws IOException {
        final ConnectionConfiguration.Builder builder = new ConnectionConfiguration.Builder(TEST_HOSTS);
        final ConnectionConfiguration connectionConfiguration = builder.build();
        assertEquals(Optional.empty(), connectionConfiguration.getProxy());
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    private PluginSetting generatePluginSetting(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure) {
        final Map<String, Object> metadata = generateConfigurationMetadata(hosts, username, password, connectTimeout, socketTimeout, awsSigv4, awsRegion, awsStsRoleArn, certPath, insecure);
        return getPluginSettingByConfigurationMetadata(metadata);
    }

    private Map<String, Object> generateConfigurationMetadata(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final boolean awsSigv4, final String awsRegion,
            final String awsStsRoleArn, final String certPath, final boolean insecure) {
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
        metadata.put("aws_sts_role_arn", awsStsRoleArn);
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);
        return metadata;
    }

    private PluginSetting getPluginSettingByConfigurationMetadata(final Map<String, Object> metadata) {
        final PluginSetting pluginSetting = new PluginSetting("opensearch", metadata);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return pluginSetting;
    }
}
