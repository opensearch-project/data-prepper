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

package com.amazon.dataprepper.plugins.prepper.grok;


import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.prepper.AbstractPrepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;


@DataPrepperPlugin(name = "grok_prepper", type = PluginType.PREPPER)
public class GrokPrepper extends AbstractPrepper<Record<String>, Record<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GrokPrepper.class);

    private final GrokPrepperConfig grokPrepperConfig;

    public GrokPrepper(final PluginSetting pluginSetting) {
        super(pluginSetting);
        grokPrepperConfig = GrokPrepperConfig.buildConfig(pluginSetting);
    }

    /**
     * execute the prepper logic which could potentially modify the incoming record. The level to which the record has
     * been modified depends on the implementation
     *
     * @param records Input records that will be modified/processed
     * @return Record  modified output records
     */
    @Override
    public Collection<Record<String>> doExecute(Collection<Record<String>> records) {
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
