/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A basic test processor implementation which tracks the list of all events processed by it
 */
@DataPrepperPlugin(name = "basic_events_tracking_test", pluginType = Processor.class)
public class BasicEventsTrackingTestProcessor extends BaseEventsTrackingProcessor {
    private static final Map<String, AtomicInteger> PROCESSED_EVENTS_MAP = new ConcurrentHashMap<>();
    private static final String PLUGIN_NAME = "basic_events_tracking_test";

    public BasicEventsTrackingTestProcessor() {
        super(PLUGIN_NAME, PROCESSED_EVENTS_MAP);
    }

    /**
     * Gets the map of processed events.
     * @return Map of event IDs to processing counts
     */
    public static Map<String, AtomicInteger> getEventsMap() {
        return PROCESSED_EVENTS_MAP;
    }

    /**
     * Gets the name of this processor.
     * @return The processor name
     */
    public static String getName() {
        return PLUGIN_NAME;
    }

    /**
     * Resets the processor's state by clearing the events map.
     */
    public static void reset() {
        PROCESSED_EVENTS_MAP.clear();
    }
}
