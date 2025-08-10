/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages output of live capture data to a configured sink.
 * Processes complete live capture traces from individual events and forwards them
 * to a live capture sink for storage or analysis. This class works with event-bound
 * live capture data and does not maintain any global state.
 */
public class LiveCaptureOutputManager {
    private static final Logger LOG = LoggerFactory.getLogger(LiveCaptureOutputManager.class);
    private static volatile LiveCaptureOutputManager INSTANCE;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private volatile Sink<Record<Event>> outputSink;

    private LiveCaptureOutputManager() {
        // Private constructor for singleton
    }

    /**
     * Gets the singleton instance of LiveCaptureOutputManager.
     *
     * @return the singleton instance
     */
    public static LiveCaptureOutputManager getInstance() {
        if (INSTANCE == null) {
            synchronized (LiveCaptureOutputManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LiveCaptureOutputManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Initializes the output manager with a configured sink.
     *
     * @param sink the sink to send live capture events to
     */
    public synchronized void initialize(final Sink<Record<Event>> sink) {
        if (initialized.get()) {
            LOG.warn("LiveCaptureOutputManager already initialized, ignoring duplicate initialization");
            return;
        }

        if (sink == null) {
            LOG.error("Cannot initialize LiveCaptureOutputManager with null sink");
            return;
        }

        this.outputSink = sink;

        // Initialize the sink
        try {
            sink.initialize();
            LOG.info("LiveCaptureOutputManager initialized with sink: {}",
                    sink.getClass().getSimpleName());
            initialized.set(true);
        } catch (Exception e) {
            LOG.error("Failed to initialize live capture output sink: {}", e.getMessage(), e);
        }
    }

    /**
     * Enables live capture output processing.
     * When enabled, event-bound live capture data will be forwarded to the configured sink.
     */
    public synchronized void enable() {
        if (!initialized.get()) {
            LOG.warn("Cannot enable LiveCaptureOutputManager - not initialized with a sink");
            return;
        }

        if (!enabled.get()) {
            enabled.set(true);
            LOG.info("LiveCaptureOutputManager enabled - processing when live capture traces complete");
        }
    }

    /**
     * Disables live capture output processing.
     */
    public synchronized void disable() {
        if (enabled.get()) {
            enabled.set(false);
            LOG.info("LiveCaptureOutputManager disabled");
        }
    }

    /**
     * Shuts down the output manager and the configured sink.
     */
    public synchronized void shutdown() {
        disable();

        if (outputSink != null) {
            try {
                outputSink.shutdown();
                LOG.info("LiveCaptureOutputManager sink shutdown completed");
            } catch (Exception e) {
                LOG.error("Error shutting down live capture output sink: {}", e.getMessage(), e);
            }
        }

        initialized.set(false);
    }

    /**
     * Checks if the output manager is enabled and ready to process entries.
     *
     * @return true if enabled and ready
     */
    public boolean isEnabled() {
        return enabled.get() && initialized.get() && outputSink != null && outputSink.isReady();
    }

    /**
     * Processes live capture data from an event that has reached the sink stage.
     * This method receives the complete live capture trace from an individual event
     * and forwards it directly to the live capture sink.
     *
     * @param liveCaptureEntries the complete live capture trace from an event
     */
    public void processEventLiveCapture(final List<Map<String, Object>> liveCaptureEntries) {
        if (!isEnabled() || liveCaptureEntries == null || liveCaptureEntries.isEmpty()) {
            return;
        }

        try {
            // Send the entries array directly to live capture sink
            Event event = JacksonEvent.builder()
                    .withEventType("live-capture-trace")
                    .withData(Map.of("entries", liveCaptureEntries))
                    .build();

            Record<Event> record = new Record<>(event);
            outputSink.output(java.util.Collections.singletonList(record));

            LOG.debug("Sent live capture trace with {} entries to sink", liveCaptureEntries.size());

        } catch (Exception e) {
            LOG.error("Error processing live capture data for event: {}", e.getMessage(), e);
        }
    }
}