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
import org.opensearch.dataprepper.plugin.TestPluginWithExperimentalFeatureConfiguration;

@DataPrepperPlugin(name = "test_plugin_with_experimental_feature", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginWithExperimentalFeatureConfiguration.class)
public class TestPluginWithExperimentalFeature implements TestPluggableInterface {
    private final TestPluginWithExperimentalFeatureConfiguration configuration;

    @DataPrepperPluginConstructor
    public TestPluginWithExperimentalFeature(final TestPluginWithExperimentalFeatureConfiguration configuration) {
        this.configuration = configuration;
    }

    public TestPluginWithExperimentalFeatureConfiguration getConfiguration() {
        return configuration;
    }
}
