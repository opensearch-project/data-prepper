package com.amazon.situp.plugins.sink.elasticsearch;

import com.amazon.situp.model.configuration.PluginSetting;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectionConfigurationTests {
    @Test
    public void testReadConnectionConfigurationDefault() {
        final List<String> testHosts = Collections.singletonList("http://localhost:9200");
        final PluginSetting pluginSetting = generatePluginSetting(
                testHosts, null, null, null, null);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(testHosts, connectionConfiguration.getHosts());
        assertNull(connectionConfiguration.getUsername());
        assertNull(connectionConfiguration.getPassword());
        assertNull(connectionConfiguration.getConnectTimeout());
        assertNull(connectionConfiguration.getSocketTimeout());
    }

    @Test
    public void testReadConnectionConfigurationCustom() {
        final List<String> testHosts = Collections.singletonList("http://localhost:9200");
        final String username = "admin";
        final String password = "admin";
        final Integer connectTimeout = 5;
        final Integer socketTimeout = 10;
        final PluginSetting pluginSetting = generatePluginSetting(
                testHosts, username, password, connectTimeout, socketTimeout);
        final ConnectionConfiguration connectionConfiguration =
                ConnectionConfiguration.readConnectionConfiguration(pluginSetting);
        assertEquals(testHosts, connectionConfiguration.getHosts());
        assertEquals(username, connectionConfiguration.getUsername());
        assertEquals(password, connectionConfiguration.getPassword());
        assertEquals(connectTimeout, connectionConfiguration.getConnectTimeout());
        assertEquals(socketTimeout, connectionConfiguration.getSocketTimeout());
    }

    private PluginSetting generatePluginSetting(
            final List<String> hosts, final String username, final String password,
            final Integer connectTimeout, final Integer socketTimeout) {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("hosts", hosts);
        metadata.put("username", username);
        metadata.put("password", password);
        metadata.put("connect_timeout", connectTimeout);
        metadata.put("socket_timeout", socketTimeout);

        return new PluginSetting("elasticsearch", metadata);
    }
}
