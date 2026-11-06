/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;

public class TestPluginWithNestedPluginConfig {
    @JsonProperty("nested_plugin")
    @UsesDataPrepperPlugin(pluginType = TestNestedPluginInterface.class)
    private TestNestedPluginInterface nestedPlugin;

    @JsonProperty("name")
    private String name;

    public TestNestedPluginInterface getNestedPlugin() {
        return nestedPlugin;
    }

    public String getName() {
        return name;
    }
}
