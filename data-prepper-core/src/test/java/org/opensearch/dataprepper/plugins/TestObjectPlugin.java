/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;

/**
 * Used for integration testing the plugin framework.
 * TODO: Move this into the org.opensearch.dataprepper.plugin package once alternate packages are supported per #379.
 */
@DataPrepperPlugin(name = "test_object_plugin", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestObjectPlugin implements TestPluggableInterface {
    private final TestPluginConfiguration configuration;
    private final Object object;

    @DataPrepperPluginConstructor
    public TestObjectPlugin(final TestPluginConfiguration configuration, Object obj) {
        this.configuration = configuration;
        this.object = obj;

    }
    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }

    public Object getObject() {
        return object;
    }
}

