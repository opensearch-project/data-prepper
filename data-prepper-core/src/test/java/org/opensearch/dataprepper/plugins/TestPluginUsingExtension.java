/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;
import org.opensearch.dataprepper.plugin.TestPluginConfiguration;
import org.opensearch.dataprepper.plugins.test.TestExtension;

/**
 * Used for integration testing the plugin framework with extensions.
 */
@DataPrepperPlugin(name = "test_plugin_using_extension", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestPluginUsingExtension implements TestPluggableInterface {
    private final TestPluginConfiguration configuration;
    private final TestExtension.TestModel extensionModel;

    @DataPrepperPluginConstructor
    public TestPluginUsingExtension(final TestPluginConfiguration configuration,
                                    final TestExtension.TestModel extensionModel) {
        this.configuration = configuration;
        this.extensionModel = extensionModel;
    }

    public TestPluginConfiguration getConfiguration() {
        return configuration;
    }

    public TestExtension.TestModel getExtensionModel() {
        return extensionModel;
    }
}
