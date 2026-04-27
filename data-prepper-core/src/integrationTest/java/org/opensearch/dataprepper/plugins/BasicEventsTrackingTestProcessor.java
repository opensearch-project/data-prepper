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
    private static final int NUMBER_OF_PROCESS_WORKERS_NOT_CAPTURED = -1;

    public BasicEventsTrackingTestProcessor() {
        super(PLUGIN_NAME, PROCESSED_EVENTS_MAP, NUMBER_OF_PROCESS_WORKERS_NOT_CAPTURED);
    }
}
