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

@DataPrepperPlugin(name = "test_plugin_with_nested", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginWithNestedPluginConfig.class)
public class TestPluginWithNestedPlugin implements TestPluggableInterface {
    private final TestPluginWithNestedPluginConfig configuration;

    @DataPrepperPluginConstructor
    public TestPluginWithNestedPlugin(final TestPluginWithNestedPluginConfig configuration) {
        this.configuration = configuration;
    }

    public TestPluginWithNestedPluginConfig getConfiguration() {
        return configuration;
    }
}
