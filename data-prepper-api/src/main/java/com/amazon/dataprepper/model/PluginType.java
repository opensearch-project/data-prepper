/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.model;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;

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
