/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.osgitestecho;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * A simple pass-through processor that logs each event and returns them unchanged.
 * Used exclusively for integration testing the OSGi plugin loading pipeline.
 * This plugin lives in data-prepper-test and is NOT included in the release distribution.
 */
@DataPrepperPlugin(name = "osgi_test_echo", pluginType = Processor.class)
public class OsgiTestEchoProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiTestEchoProcessor.class);

    @DataPrepperPluginConstructor
    public OsgiTestEchoProcessor(final PluginMetrics pluginMetrics) {
        super(pluginMetrics);
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            LOG.debug("osgi_test_echo: {}", record.getData().toJsonString());
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
    }
}
