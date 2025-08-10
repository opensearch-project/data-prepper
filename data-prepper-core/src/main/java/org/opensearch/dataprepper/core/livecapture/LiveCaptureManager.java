/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages live capture functionality for Data Prepper events.
 * Coordinates event marking, rate limiting, and capture entry management.
 */
public class LiveCaptureManager {
    private static final Logger LOG = LoggerFactory.getLogger(LiveCaptureManager.class);
    public static final String LIVE_CAPTURE_FLAG = "liveCapture";
    public static final String LIVE_CAPTURE_OUTPUT = "liveCaptureOutput";
    public static final String SOURCE = "Source";
    public static final String PROCESSOR = "Processor";
    public static final String ROUTE = "Route";
    public static final String SINK = "Sink";
    private static volatile LiveCaptureManager INSTANCE;

    private final AtomicBoolean enabled;
    private volatile RateLimiter rateLimiter;
    private volatile double currentRateLimit;
    private final Map<String, String> activeFilters;

    private LiveCaptureManager(final boolean initialEnabled, final double eventsPerSecond) {
        this.enabled = new AtomicBoolean(initialEnabled);
        this.rateLimiter = RateLimiter.create(eventsPerSecond);
        this.currentRateLimit = eventsPerSecond;
        this.activeFilters = new ConcurrentHashMap<>();
    }

    /**
     * Initializes the singleton instance of LiveCaptureManager.
     *
     * @param initialEnabled whether live capture is initially enabled
     * @param eventsPerSecond the initial rate limit for events per second
     */
    public static synchronized void initialize(final boolean initialEnabled, final double eventsPerSecond) {
        if (INSTANCE == null) {
            INSTANCE = new LiveCaptureManager(initialEnabled, eventsPerSecond);
            LOG.info("LiveCaptureManager initialized: enabled={}, rate={}", initialEnabled, eventsPerSecond);
        } else {
            // Allow updating enabled state and rate after initialization
            INSTANCE.enabled.set(initialEnabled);
            INSTANCE.rateLimiter = RateLimiter.create(eventsPerSecond);
            INSTANCE.currentRateLimit = eventsPerSecond;
            LOG.info("LiveCaptureManager updated: enabled={}, rate={}", initialEnabled, eventsPerSecond);
        }
    }

    /**
     * @return the singleton instance of LiveCaptureManager
     */
    public static synchronized LiveCaptureManager getInstance() {
        if (INSTANCE == null) {
            initialize(false, 1.0);
        }
        return INSTANCE;
    }

    /**
     * Checks if an event is marked for live capture via metadata.
     *
     * @param event the event to check
     * @return true if the event is marked for live capture
     */
    public static boolean shouldLiveCapture(final Event event) {
        Object flag = event.getMetadata().getAttribute(LIVE_CAPTURE_FLAG);
        return Boolean.TRUE.equals(flag);
    }

    /**
     * Marks an event for live capture and creates the output list.
     *
     * @param event the event to mark
     * @param liveCapture whether to mark the event for live capture
     */
    public static void setLiveCapture(final Event event, final boolean liveCapture) {
        final EventMetadata md = event.getMetadata();
        md.setAttribute(LIVE_CAPTURE_FLAG, liveCapture);
        if (liveCapture && md.getAttribute(LIVE_CAPTURE_OUTPUT) == null) {
            md.setAttribute(LIVE_CAPTURE_OUTPUT, new ArrayList<Map<String, Object>>());
        }
    }

    public static void addLiveCaptureEntry(
            final Event event,
            final String stage,
            final String description,
            final String name,
            final String pipelineName) {

        if (!shouldLiveCapture(event)) return;

        Map<String, Object> entry = new HashMap<>();
        entry.put("stage", stage);
        entry.put("eventTime", Instant.now().toString());
        entry.put("logMessage", description);

        if (SOURCE.equals(stage) && pipelineName != null) {
            entry.put("pipelineName", pipelineName);
        }

        Map<String, Object> captureMetaData = createCaptureMetadata(event, stage, name);
        entry.put("captureMetaData", captureMetaData);

        List<Map<String, Object>> output = (List<Map<String, Object>>) event.getMetadata().getAttribute(LIVE_CAPTURE_OUTPUT);
        if (output != null) {
            output.add(entry);
        }
    }

    /**
     * Gets the live capture output list from an event.
     *
     * @param event the event to get the output from
     * @return the live capture output list
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getLiveCaptureOutput(final Event event) {
        return (List<Map<String, Object>>) event.getMetadata().getAttribute(LIVE_CAPTURE_OUTPUT);
    }

    /**
     * Turns live capture on or off with control over filter preservation.
     *
     * @param flag whether to enable or disable live capture
     * @param clearFilters whether to clear filters when disabling (ignored when enabling)
     */
    public void setEnabled(final boolean flag, final boolean clearFilters) {
        enabled.set(flag);
        if (!flag && clearFilters) {
            activeFilters.clear();
            LOG.info("Live capture disabled and filters cleared");
        } else {
            LOG.info("Live capture {} (filters {})",
                flag ? "enabled" : "disabled",
                activeFilters.isEmpty() ? "none" : "preserved");
        }
    }

    /**
     * Turns live capture on or off. Disabling clears all filters by default.
     *
     * @param flag whether to enable or disable live capture
     */
    public void setEnabled(final boolean flag) {
        setEnabled(flag, true); // Default behavior: clear filters when disabling
    }

    /**
     * @return true if live capture is enabled
     */
    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Updates the global rate limit (events per second).
     *
     * @param eps the new events per second rate
     */
    public void setRateLimit(final double eps) {
        rateLimiter = RateLimiter.create(eps);
        currentRateLimit = eps;
        LOG.info("Live capture rate set to {} eps", eps);
    }

    /**
     * @return the current rate limit in events per second
     */
    public double getRateLimit() {
        return currentRateLimit;
    }

    /**
     * Adds a filter for selective event capture.
     *
     * @param name the filter name
     * @param expr the filter expression
     */
    public void addFilter(final String name, final String expr) {
        activeFilters.put(name, expr);
        LOG.info("Live-capture filter added: {} = {}", name, expr);
    }

    /**
     * Clears all active filters.
     */
    public void clearFilters() {
        activeFilters.clear();
        LOG.info("Live-capture filters cleared");
    }

    /**
     * @return true if there are active filters for selective event capture
     */
    public boolean hasActiveFilters() {
        return !activeFilters.isEmpty();
    }

    /**
     * Decides whether this event should be marked at birth.
     *
     * @param event the event to evaluate
     * @return true if the event should be captured
     */
    public boolean shouldCaptureEvent(final Event event) {
        if (!enabled.get()) {
            return false;
        }
        return activeFilters.isEmpty() || matchesFilters(event);
    }

    /**
     * Decides whether this event should be marked at birth, including rate limiting.
     * This method should only be called at the Source stage when events are first created.
     *
     * @param event the event to evaluate
     * @return true if the event should be captured after applying rate limiting
     */
    public boolean shouldCaptureEventWithRateLimit(final Event event) {
        if (!shouldCaptureEvent(event)) {
            return false;
        }

        // Apply rate limiting only at event creation time
        return rateLimiter.tryAcquire();
    }

    private static Map<String, Object> createCaptureMetadata(Event event, String stage, String name) {
        Map<String, Object> captureMetaData = new HashMap<>();
        captureMetaData.put("event", event.toMap());

        switch (stage) {
            case SOURCE:
                String ingestionMethod = extractIngestionMethod(event);
                if (ingestionMethod != null) {
                    captureMetaData.put("ingestionMethod", ingestionMethod);
                }
                break;
            case PROCESSOR:
                if (name != null) captureMetaData.put("processorName", name);
                break;
            case ROUTE:
                if (name != null) captureMetaData.put("routeName", name);
                break;
            case SINK:
                if (name != null) captureMetaData.put("sinkName", name);
                break;
        }

        return captureMetaData;
    }

    private boolean matchesFilters(final Event event) {
        if (activeFilters.isEmpty()) return true;

        try {
            Map<String, Object> eventData = event.toMap();
            return activeFilters.entrySet().stream().allMatch(filter -> {
                Object actualValue = eventData.get(filter.getKey());
                return actualValue != null && filter.getValue().equals(actualValue.toString());
            });
        } catch (Exception e) {
            LOG.warn("Error checking event filters: {}", e.getMessage());
            return false;
        }
    }

    private static String extractIngestionMethod(final Event event) {
        EventMetadata metadata = event.getMetadata();
        Object ingestionMethod = metadata.getAttribute("ingestionMethod");
        if (ingestionMethod != null) {
            return ingestionMethod.toString();
        }

        String eventType = metadata.getEventType();
        if (eventType != null) {
            return eventType;
        }

        return "unknown";
    }

}