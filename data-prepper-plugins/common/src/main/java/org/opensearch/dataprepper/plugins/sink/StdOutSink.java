/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "stdout", pluginType = Sink.class)
public class StdOutSink implements Sink<Record<Object>> {

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StdOutSink} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StdOutSink(final PluginSetting pluginSetting) {
        this();
    }

    public StdOutSink() {
    }

    @Override
    public void output(final Collection<Record<Object>> records) {
        for (final Record<Object> record : records) {
            checkTypeAndPrintObject(record.getData());
        }
    }

    // Temporary function to support both trace and log ingestion pipelines.
    // TODO: This function should be removed with the completion of: https://github.com/opensearch-project/data-prepper/issues/546
    private void checkTypeAndPrintObject(final Object object) {
        if (object instanceof Event) {
            System.out.println(((Event) object).toJsonString());
            EventHandle eventHandle = ((Event)object).getEventHandle();
            if (eventHandle != null) {
                eventHandle.release(true);
            }
        } else {
            System.out.println(object);
        }
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void initialize() {
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
