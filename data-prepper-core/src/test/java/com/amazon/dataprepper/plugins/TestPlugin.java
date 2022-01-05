/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.plugin.TestPluggableInterface;
import com.amazon.dataprepper.plugin.TestPluginConfiguration;

/**
 * Used for integration testing the plugin framework.
 * TODO: Move this into the com.amazon.dataprepper.plugin package once alternate packages are supported per #379.
 */
@DataPrepperPlugin(name = "test_plugin", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestPlugin implements TestPluggableInterface {
    private final TestPluginConfiguration configuration;

    @DataPrepperPluginConstructor
    public TestPlugin(final TestPluginConfiguration configuration) {
        this.configuration = configuration;
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }
}
