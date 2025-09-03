/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.model.event.Event;
import org.springframework.stereotype.Component;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.dataprepper.model.buffer.Buffer;

import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;

/**
 * Manages live capture functionality for Data Prepper events.
 * Handles event marking, filtering, rate limiting, capture entry management, and output processing.
 */
@Component
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
    private final Map<String, Buffer<Record<Event>>> pipelineBuffers = new ConcurrentHashMap<>();

    public LiveCaptureManager() {
        this.enabled = new AtomicBoolean(false);
        this.rateLimiter = RateLimiter.create(1.0);
        this.currentRateLimit = 1.0;
        this.activeFilters = new ConcurrentHashMap<>();
    }

    public void initialize(final boolean initialEnabled, final double eventsPerSecond) {
        this.enabled.set(initialEnabled);
        this.rateLimiter = RateLimiter.create(eventsPerSecond);
        this.currentRateLimit = eventsPerSecond;
        // Set the static instance for backward compatibility
        INSTANCE = this;
        LOG.info("LiveCaptureManager initialized: enabled={}, rate={}", initialEnabled, eventsPerSecond);
    }

    // Static methods for backward compatibility with non-Spring managed classes
    public static synchronized LiveCaptureManager getInstance() {
        if (INSTANCE == null) {
            // Fallback instance for non-Spring contexts
            INSTANCE = new LiveCaptureManager();
            INSTANCE.initialize(false, 1.0);
        }
        return INSTANCE;
    }
    public static boolean shouldLiveCapture(final Event event) {
        Object flag = event.getMetadata().getAttribute(LIVE_CAPTURE_FLAG);
        return TRUE.equals(flag);
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
    public static void processEventLiveCapture(final List<Map<String, Object>> liveCaptureEntries) {
        LiveCaptureManager instance = getInstance();
        if (!instance.enabled.get() || instance.outputSink == null || liveCaptureEntries == null || liveCaptureEntries.isEmpty()) {
            return;
        }

        try {
            Event event = JacksonEvent.builder()
                    .withEventType("LIVE_CAPTURE_TRACE")
                    .withData(Map.of("entries", liveCaptureEntries))
                    .build();

            instance.outputSink.output(singletonList(new Record<>(event)));
            LOG.debug("Sent live capture trace with {} entries to sink", liveCaptureEntries.size());
        } catch (Exception e) {
            LOG.error("Error processing live capture data: {}", e.getMessage(), e);
        }
    }

    // Instance method implementations
    /**
     * Capture events from source stage
     */
    public static void captureSourceEvents(Collection<Record<Event>> records, Object source, String pipelineName) {
        LiveCaptureManager instance = getInstance();
        if (!instance.isEnabled()) return;
        
        String sourceName = extractPluginName(source, "Source");
        
        for (Record<Event> eventRecord : records) {
            if (eventRecord.getData() instanceof Event) {
                Event event = eventRecord.getData();
                
                if (!shouldLiveCapture(event) && instance.shouldCaptureEventWithRateLimit(event)) {
                    setLiveCapture(event, true);
                    addLiveCaptureEntry(event, SOURCE, 
                        "Event captured from " + sourceName + " source", null, pipelineName);
                }
            }
        }
    }
    
    /**
     * Capture events from processor stage
     */
    public static void captureProcessorEvents(Collection<Record<Event>> records, Object processor) {
        LiveCaptureManager instance = getInstance();
        if (!instance.isEnabled()) return;
        
        String processorName = extractPluginName(processor, "Processor");
        
        for (Record<Event> eventRecord : records) {
            if (eventRecord.getData() instanceof Event) {
                Event event = eventRecord.getData();
                
                if (shouldLiveCapture(event)) {
                    addLiveCaptureEntry(event, PROCESSOR,
                        "After " + processorName + " processor execution", processorName, null);
                }
            }
        }
    }
    
    /**
     * Capture route information
     */
    public static void captureRouteEvents(Collection<Record> allRecords, Map<Record, Set<String>> recordsToRoutes) {
        LiveCaptureManager instance = getInstance();
        if (!instance.isEnabled()) return;
        
        for (Record eventRecord : allRecords) {
            if (eventRecord.getData() instanceof Event) {
                Event event = (Event) eventRecord.getData();
                
                if (shouldLiveCapture(event)) {
                    Set<String> matchedRoutes = recordsToRoutes.get(eventRecord);
                    
                    if (matchedRoutes != null && !matchedRoutes.isEmpty()) {
                        for (String routeName : matchedRoutes) {
                            addLiveCaptureEntry(event, ROUTE,
                                "Event matched route: " + routeName, routeName, null);
                        }
                    } else {
                        addLiveCaptureEntry(event, ROUTE,
                            "Event matched no routes", "_no_route", null);
                    }
                }
            }
        }
    }
    
    /**
     * Capture sink events
     */
    public static void captureSinkEvents(Collection<Record> events, Object sink) {
        LiveCaptureManager instance = getInstance();
        if (!instance.isEnabled()) return;
        
        String sinkName = extractPluginName(sink, "Sink");
        
        for (Record eventRecord : events) {
            if (eventRecord.getData() instanceof Event) {
                Event event = (Event) eventRecord.getData();
                
                if (shouldLiveCapture(event)) {
                    addLiveCaptureEntry(event, SINK,
                        "Received by sink " + sinkName, sinkName, null);
                }
            }
        }
    }
    
    /**
     * Process all live capture data after pipeline completion
     */
    public static void processCompletedEvents(Collection<Record<Event>> records) {
        LiveCaptureManager instance = getInstance();
        if (!instance.isEnabled()) return;

        for (Record<Event> eventRecord : records) {
            if (eventRecord.getData() instanceof Event) {
                Event event = eventRecord.getData();
                
                if (shouldLiveCapture(event)) {
                    List<Map<String, Object>> liveCaptureEntries = getLiveCaptureOutput(event);
                    if (liveCaptureEntries != null && !liveCaptureEntries.isEmpty()) {
                        processEventLiveCapture(liveCaptureEntries);
                    }
                }
            }
        }
    }
    
    /**
     * Gets the buffer for a specific pipeline by name.
     */
    public static Buffer<Record<Event>> getPipelineBuffer(final String pipelineName) {
        return getInstance().pipelineBuffers.get(pipelineName);
    }

    /**
     * Gets all available pipeline names.
     */
    public static Set<String> getAvailablePipelines() {
        return getInstance().pipelineBuffers.keySet();
    }
    
    /**
     * Stores a pipeline buffer for event injection.
     */
    public static void storePipelineBuffer(final String pipelineName, final Buffer<Record<Event>> buffer) {
        getInstance().pipelineBuffers.put(pipelineName, buffer);
    }
    
    /**
     * Consolidated method to extract plugin names from instances
     */
    private static String extractPluginName(Object plugin, String suffix) {
        if (plugin == null) return "unknown";
        
        String className = plugin.getClass().getSimpleName();
        
        // Remove suffix
        if (className.endsWith(suffix)) {
            className = className.substring(0, className.length() - suffix.length());
        }
        
        // Convert CamelCase to snake_case for processors, lowercase for others
        if ("Processor".equals(suffix)) {
            return className.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
        } else {
            return className.toLowerCase();
        }
    }

}