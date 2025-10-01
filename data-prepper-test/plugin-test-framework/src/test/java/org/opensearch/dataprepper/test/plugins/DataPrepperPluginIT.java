/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins;

import org.opensearch.dataprepper.plugins.test.TestPluggableInterface;
import org.opensearch.dataprepper.test.plugins.junit.BaseDataPrepperPluginStandardTestSuite;

@DataPrepperPluginTest(pluginName = "test_plugin", pluginType = TestPluggableInterface.class)
class DataPrepperPluginIT extends BaseDataPrepperPluginStandardTestSuite {
}
