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
import org.opensearch.dataprepper.model.configuration.PluginModel;

public class TestPluginWithPluginModelConfig {
    @JsonProperty("name")
    private String name;

    @JsonProperty("nested_action")
    @UsesDataPrepperPlugin(pluginType = TestNestedPluginInterface.class)
    private PluginModel nestedAction;

    public String getName() {
        return name;
    }

    public PluginModel getNestedAction() {
        return nestedAction;
    }
}
