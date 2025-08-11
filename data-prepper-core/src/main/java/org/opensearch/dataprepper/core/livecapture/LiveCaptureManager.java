/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
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
 * Handles event marking, filtering, rate limiting, capture entry management, and output processing.
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
    private volatile Sink<Record<Event>> outputSink;

    private LiveCaptureManager(final boolean initialEnabled, final double eventsPerSecond) {
        this.enabled = new AtomicBoolean(initialEnabled);
        this.rateLimiter = RateLimiter.create(eventsPerSecond);
        this.currentRateLimit = eventsPerSecond;
        this.activeFilters = new ConcurrentHashMap<>();
    }

    public static synchronized void initialize(final boolean initialEnabled, final double eventsPerSecond) {
        if (INSTANCE == null) {
            INSTANCE = new LiveCaptureManager(initialEnabled, eventsPerSecond);
        } else {
            INSTANCE.enabled.set(initialEnabled);
            INSTANCE.rateLimiter = RateLimiter.create(eventsPerSecond);
            INSTANCE.currentRateLimit = eventsPerSecond;
        }
        LOG.info("LiveCaptureManager {}: enabled={}, rate={}", 
                INSTANCE == null ? "initialized" : "updated", initialEnabled, eventsPerSecond);
    }

    public static synchronized LiveCaptureManager getInstance() {
        if (INSTANCE == null) {
            initialize(false, 1.0);
        }
        return INSTANCE;
    }

    public static boolean shouldLiveCapture(final Event event) {
        Object flag = event.getMetadata().getAttribute(LIVE_CAPTURE_FLAG);
        return Boolean.TRUE.equals(flag);
    }

    public static void setLiveCapture(final Event event, final boolean liveCapture) {
        final EventMetadata md = event.getMetadata();
        md.setAttribute(LIVE_CAPTURE_FLAG, liveCapture);
        if (liveCapture && md.getAttribute(LIVE_CAPTURE_OUTPUT) == null) {
            md.setAttribute(LIVE_CAPTURE_OUTPUT, new ArrayList<Map<String, Object>>());
        }
    }

    public static void addLiveCaptureEntry(final Event event, final String stage, 
            final String description, final String name, final String pipelineName) {
        if (!shouldLiveCapture(event)) return;

        Map<String, Object> entry = new HashMap<>();
        entry.put("stage", stage);
        entry.put("eventTime", Instant.now().toString());
        entry.put("logMessage", description);
        entry.put("captureMetaData", createCaptureMetadata(event, stage, name));

        if (SOURCE.equals(stage) && pipelineName != null) {
            entry.put("pipelineName", pipelineName);
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> output = (List<Map<String, Object>>) event.getMetadata().getAttribute(LIVE_CAPTURE_OUTPUT);
        if (output != null) {
            output.add(entry);
        }
    }

    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> getLiveCaptureOutput(final Event event) {
        return (List<Map<String, Object>>) event.getMetadata().getAttribute(LIVE_CAPTURE_OUTPUT);
    }

    public void setEnabled(final boolean flag, final boolean clearFilters) {
        enabled.set(flag);
        if (!flag && clearFilters) {
            activeFilters.clear();
        }
        LOG.info("Live capture {} (filters {})", 
                flag ? "enabled" : "disabled",
                !flag && clearFilters ? "cleared" : 
                activeFilters.isEmpty() ? "none" : "preserved");
    }


    public void setEnabled(final boolean flag) {
        setEnabled(flag, true);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setRateLimit(final double eps) {
        rateLimiter = RateLimiter.create(eps);
        currentRateLimit = eps;
        LOG.info("Live capture rate set to {} eps", eps);
    }

    public double getRateLimit() {
        return currentRateLimit;
    }


    public void addFilter(final String name, final String expr) {
        activeFilters.put(name, expr);
        LOG.info("Live-capture filter added: {} = {}", name, expr);
    }

    public void clearFilters() {
        activeFilters.clear();
        LOG.info("Live-capture filters cleared");
    }

    public boolean hasActiveFilters() {
        return !activeFilters.isEmpty();
    }

    public boolean shouldCaptureEvent(final Event event) {
        return enabled.get() && (activeFilters.isEmpty() || matchesFilters(event));
    }

    public boolean shouldCaptureEventWithRateLimit(final Event event) {
        return shouldCaptureEvent(event) && rateLimiter.tryAcquire();
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

    public void setOutputSink(final Sink<Record<Event>> sink) {
        this.outputSink = sink;
        LOG.info("Live capture output sink configured: {}",
                sink != null ? sink.getClass().getSimpleName() : "null");
    }

    // turn every entry into a dataprepper event so we can output to sink
    public void processEventLiveCapture(final List<Map<String, Object>> liveCaptureEntries) {
        if (!enabled.get() || outputSink == null || liveCaptureEntries == null || liveCaptureEntries.isEmpty()) {
            return;
        }

        try {
            Event event = JacksonEvent.builder()
                    .withEventType("live-capture-trace")
                    .withData(Map.of("entries", liveCaptureEntries))
                    .build();

            outputSink.output(java.util.Collections.singletonList(new Record<>(event)));
            LOG.debug("Sent live capture trace with {} entries to sink", liveCaptureEntries.size());
        } catch (Exception e) {
            LOG.error("Error processing live capture data: {}", e.getMessage(), e);
        }
    }

}