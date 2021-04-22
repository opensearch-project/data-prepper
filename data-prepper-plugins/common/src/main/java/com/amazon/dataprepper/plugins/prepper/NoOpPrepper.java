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

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "no-op", type = PluginType.PREPPER)
public class NoOpPrepper<InputT extends Record<?>> implements Prepper<InputT, InputT> {

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link NoOpPrepper} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public NoOpPrepper(final PluginSetting pluginSetting) {
        //no op
    }

    public NoOpPrepper() {

    }

    @Override
    public Collection<InputT> execute(Collection<InputT> records) {
        return records;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }


    @Override
    public void shutdown() {

    }
}
