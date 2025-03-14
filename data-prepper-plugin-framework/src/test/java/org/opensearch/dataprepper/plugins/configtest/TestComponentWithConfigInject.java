package org.opensearch.dataprepper.plugins.configtest;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;

import javax.inject.Named;

@Named
public class TestComponentWithConfigInject {
    private final TestPluginConfiguration configuration;
    private final PluginMetrics pluginMetrics;

    public TestComponentWithConfigInject(TestPluginConfiguration configuration, PluginMetrics pluginMetrics) {
        this.configuration = configuration;
        this.pluginMetrics = pluginMetrics;
    }

    public String getIdentifier() {
        return "test-component-with-plugin-config-injected";
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }

    public PluginMetrics getPluginMetrics() {
        return pluginMetrics;
    }
}
