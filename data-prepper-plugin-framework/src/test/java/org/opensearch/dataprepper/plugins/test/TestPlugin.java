/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;

/**
 * Used for integration testing the plugin framework.
 * TODO: Move this into the org.opensearch.dataprepper.plugin package once alternate packages are supported per #379.
 */
@DataPrepperPlugin(name = "test_plugin", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestPlugin implements TestPluggableInterface {
    private final TestPluginConfiguration configuration;
    private final Object obj;

    @DataPrepperPluginConstructor
    public TestPlugin(final TestPluginConfiguration configuration) {
        this.configuration = configuration;
        this.obj = null;
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }

    public Object getObject() {
        return obj;
    }
}
