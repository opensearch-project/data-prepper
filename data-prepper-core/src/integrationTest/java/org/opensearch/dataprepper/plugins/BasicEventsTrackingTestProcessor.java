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
}
