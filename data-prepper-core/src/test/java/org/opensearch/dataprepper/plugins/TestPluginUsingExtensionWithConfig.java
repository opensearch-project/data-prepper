/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;
import org.opensearch.dataprepper.plugins.test.TestExtensionWithConfig;

/**
 * Used for integration testing the plugin framework with extensions.
 */
@DataPrepperPlugin(name = "test_plugin_using_extension_with_config", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestPluginUsingExtensionWithConfig implements TestPluggableInterface {
    private final TestPluginConfiguration configuration;
    private final TestExtensionWithConfig.TestModel extensionModel;

    @DataPrepperPluginConstructor
    public TestPluginUsingExtensionWithConfig(final TestPluginConfiguration configuration,
                                              final TestExtensionWithConfig.TestModel extensionModel) {
        this.configuration = configuration;
        this.extensionModel = extensionModel;
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }

    public TestExtensionWithConfig.TestModel getExtensionModel() {
        return extensionModel;
    }
}
