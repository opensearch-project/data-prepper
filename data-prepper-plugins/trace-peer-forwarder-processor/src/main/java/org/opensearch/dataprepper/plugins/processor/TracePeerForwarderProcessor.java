/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;

@DataPrepperPlugin(name = "trace_peer_forwarder", pluginType = Processor.class)
public class TracePeerForwarderProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {

    @DataPrepperPluginConstructor
    public TracePeerForwarderProcessor(PluginMetrics pluginMetrics) {
        super(pluginMetrics);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
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

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singleton("traceId");
    }
}
