/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.processor.Processor;

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

    public SingleThreadEventsTrackingTestProcessor() {
        super(PLUGIN_NAME, PROCESSED_EVENTS_MAP);
    }
}
