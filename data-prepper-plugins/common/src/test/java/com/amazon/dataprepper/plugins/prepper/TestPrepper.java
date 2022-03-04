/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@SingleThread
@DataPrepperPlugin(name = "test_prepper", pluginType = Prepper.class)
public class TestPrepper implements Processor<Record<Event>, Record<Event>> {
    public boolean isShutdown = false;

    public TestPrepper(final PluginSetting pluginSetting) {}

    @Override
    public Collection<Record<Event>> execute(Collection<Record<Event>> records) {
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
        isShutdown = true;
    }
}
