/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;

@DataPrepperPlugin(name = "test_experimental_plugin", pluginType = TestPluggableInterface.class)
@Experimental
public class TestExperimentalPlugin {
}
