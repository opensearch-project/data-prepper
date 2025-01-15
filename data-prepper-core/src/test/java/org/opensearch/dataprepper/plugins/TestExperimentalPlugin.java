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
import org.opensearch.dataprepper.model.annotations.Experimental;
import org.opensearch.dataprepper.plugin.TestPluggableInterface;

@DataPrepperPlugin(name = "test_experimental_plugin", pluginType = TestPluggableInterface.class)
@Experimental
public class TestExperimentalPlugin {
}
