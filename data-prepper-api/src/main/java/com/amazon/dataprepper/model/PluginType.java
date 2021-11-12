/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;

/**
 * @deprecated since 1.2. This enumeration should no longer be used
 * by {@link DataPrepperPlugin} classes. They should use
 * {@link DataPrepperPlugin#pluginType()} instead.
 */
@Deprecated
public enum PluginType {
    NONE("none", Void.class),
    @Deprecated
    SOURCE("source", Source.class),
    @Deprecated
    BUFFER("buffer", Buffer.class),
    @Deprecated
    PREPPER("prepper", Prepper.class),
    @Deprecated
    SINK("sink", Sink.class);

    private final String pluginName;
    private final Class<?> pluginClass;

    PluginType(final String pluginName, final Class<?> pluginClass) {
        this.pluginName = pluginName;
        this.pluginClass = pluginClass;
    }

    public String pluginName() {
        return pluginName;
    }

    public Class<?> pluginClass() {
        return pluginClass;
    }
}
