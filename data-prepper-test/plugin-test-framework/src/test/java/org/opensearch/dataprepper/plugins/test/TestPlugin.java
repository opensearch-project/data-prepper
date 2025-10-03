/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;

@DataPrepperPlugin(name = "test_plugin", pluginType = TestPluggableInterface.class, pluginConfigurationType = TestPluginConfiguration.class)
public class TestPlugin {
    @DataPrepperPluginConstructor
    public TestPlugin(final TestPluginConfiguration configuration) {
    }
}
