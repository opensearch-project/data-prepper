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

@DataPrepperPlugin(name = "test_nested_plugin", pluginType = TestNestedPluginInterface.class, pluginConfigurationType = TestNestedPluginConfig.class)
public class TestNestedPlugin implements TestNestedPluginInterface {
    private final TestNestedPluginConfig config;

    @DataPrepperPluginConstructor
    public TestNestedPlugin(final TestNestedPluginConfig config) {
        this.config = config;
    }

    @Override
    public String getValue() {
        return config.getTestValue();
    }
}
