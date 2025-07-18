/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.common;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

@SingleThread
@DataPrepperPlugin(name = "test_processor", pluginType = Processor.class)
public class TestProcessor implements Processor<Record<String>, Record<String>> {
    public boolean isShutdown = false;
    private final PluginSetting pluginSetting;

    public TestProcessor(final PluginSetting pluginSetting) {
        this.pluginSetting = pluginSetting;
    }

    @Override
    public Collection<Record<String>> execute(Collection<Record<String>> records) {
        if (pluginSetting.getSettings().containsKey("execute_should_throw")) {
            throw new RuntimeException("throwing runtime exception");
        }
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
