/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.test.plugins.junit;

import java.util.Objects;

class DataPrepperPluginTestContext {
    private final String pluginName;
    private final Class<?> pluginType;

    DataPrepperPluginTestContext(
            final String pluginName,
            final Class<?> pluginType) {
        this.pluginName = Objects.requireNonNull(pluginName);
        this.pluginType = Objects.requireNonNull(pluginType);
    }

    public String getPluginName() {
        return pluginName;
    }

    public Class<?> getPluginType() {
        return pluginType;
    }
}
