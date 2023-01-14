/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.otellogs;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;


@DataPrepperPlugin(name = "otel_logs_raw_processor", pluginType = Processor.class)
public class OTelLogsRawProcessor extends AbstractProcessor<Record<OpenTelemetryLog>, Record<OpenTelemetryLog>> {


    @DataPrepperPluginConstructor
    public OTelLogsRawProcessor(PluginSetting pluginSetting) {
        super(pluginSetting);
    }

    @Override
    public Collection<Record<OpenTelemetryLog>> doExecute(Collection<Record<OpenTelemetryLog>> records) {
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
