package com.amazon.dataprepper.plugins.sink.elasticsearch;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ConnectionConfigurationTests {
    private final List<String> TEST_HOSTS = Collections.singletonList("http://localhost:9200");
    private final String TEST_USERNAME = "admin";
    private final String TEST_PASSWORD = "admin";
    private final Integer TEST_CONNECT_TIMEOUT = 5;
    private final Integer TEST_SOCKET_TIMEOUT = 10;
    private final String TEST_CERT_PATH = Objects.requireNonNull(getClass().getClassLoader().getResource("root-ca.pem")).getFile();

    @Test
    public void testReadConnectionConfigurationDefault() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertNull(connectionConfiguration.getCertPath());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
    }

    @Test
    public void testCreateClientDefault() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, null, null, null, null, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testReadConnectionConfigurationNoCert() {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(TEST_HOSTS, connectionConfiguration.getHosts());
        assertEquals(TEST_USERNAME, connectionConfiguration.getUsername());
        assertEquals(TEST_PASSWORD, connectionConfiguration.getPassword());
        assertEquals(TEST_CONNECT_TIMEOUT, connectionConfiguration.getConnectTimeout());
        assertEquals(TEST_SOCKET_TIMEOUT, connectionConfiguration.getSocketTimeout());
    }

    @Test
    public void testCreateClientNoCert() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, null, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientInsecure() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, null, true);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testCreateClientWithCertPath() throws IOException {
        final PluginSetting pluginSetting = generatePluginSetting(
                TEST_HOSTS, TEST_USERNAME, TEST_PASSWORD, TEST_CONNECT_TIMEOUT, TEST_SOCKET_TIMEOUT, TEST_CERT_PATH, false);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        final RestHighLevelClient client = connectionConfiguration.createClient();
        assertNotNull(client);
        client.close();
    }

    private PluginSetting generatePluginSetting(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout, final String certPath, final boolean insecure) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("hosts", hosts);
        metadata.put("username", username);
        metadata.put("password", password);
        metadata.put("connect_timeout", connectTimeout);
        metadata.put("socket_timeout", socketTimeout);
        metadata.put("cert", certPath);
        metadata.put("insecure", insecure);

        return new PluginSetting("elasticsearch", metadata);
    }
}
