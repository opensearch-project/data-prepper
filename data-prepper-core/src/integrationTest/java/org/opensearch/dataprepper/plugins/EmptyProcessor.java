/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * A test processor with no mandatory configuration parameters.
 * Each instance increments a "empty_processor_count" field on the event.
 * <p>
 * This processor is used in integration tests to verify that plugins
 * configured as a bare key, explicit null, or explicit empty string
 * all result in valid processor instances that process events.
 * <p>
 * If 3 instances are configured in the pipeline, after processing
 * the "empty_processor_count" field on each event should equal 3.
 */
@DataPrepperPlugin(name = "empty_processor", pluginType = Processor.class, pluginConfigurationType = EmptyProcessorConfig.class)
public class EmptyProcessor implements Processor<Record<Event>, Record<Event>> {
    public static final String COUNT_KEY = "empty_processor_count";

    @DataPrepperPluginConstructor
    public EmptyProcessor(final EmptyProcessorConfig config) {
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Integer currentCount = record.getData().get(COUNT_KEY, Integer.class);
            record.getData().put(COUNT_KEY, (currentCount == null ? 0 : currentCount) + 1);
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
