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
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A @SingleThread test processor implementation which tracks the list of all events processed by it
 */
@SingleThread
@DataPrepperPlugin(name = "single_thread_events_tracking_test", pluginType = Processor.class)
public class SingleThreadEventsTrackingTestProcessor extends BaseEventsTrackingProcessor {
    private static final String PLUGIN_NAME = "single_thread_events_tracking_test";
    private static final Map<String, AtomicInteger> PROCESSED_EVENTS_MAP = new ConcurrentHashMap<>();

    private static final List<SingleThreadEventsTrackingTestProcessor> PROCESSORS = new ArrayList<>();

    @DataPrepperPluginConstructor
    public SingleThreadEventsTrackingTestProcessor(final PipelineDescription pipelineDescription) {
        super(PLUGIN_NAME, PROCESSED_EVENTS_MAP, pipelineDescription.getNumberOfProcessWorkers());
        PROCESSORS.add(this);
    }

    /**
     * This is used only for testing in the setup.
     */
    public SingleThreadEventsTrackingTestProcessor() {
        super(PLUGIN_NAME, PROCESSED_EVENTS_MAP, -1);
    }

    public static List<SingleThreadEventsTrackingTestProcessor> getProcessors() {
        return PROCESSORS;
    }
}
