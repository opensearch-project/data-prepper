package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.plugin.TestPluginConfiguration;

import javax.inject.Named;

@Named
public class TestComponentWithConfigInject {
    private final TestPluginConfiguration configuration;

    public TestComponentWithConfigInject(TestPluginConfiguration configuration) {
        this.configuration = configuration;
    }

    public String getIdentifier() {
        return "test-component-with-plugin-config-injected";
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }
}
