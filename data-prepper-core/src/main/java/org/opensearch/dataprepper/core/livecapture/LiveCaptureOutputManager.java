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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages output of live capture entries to a configured sink.
 * Groups entries by traceId and sends complete traces as single events to the sink.
 * Provides timeout handling for incomplete traces and maintains backwards compatibility
 * with threshold-based processing for entries without traceId.
 */
public class LiveCaptureOutputManager {
    private static final Logger LOG = LoggerFactory.getLogger(LiveCaptureOutputManager.class);
    
    private static volatile LiveCaptureOutputManager INSTANCE;
    
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    
    private volatile Sink<Record<Event>> outputSink;
    private final ConcurrentLinkedQueue<Map<String, Object>> pendingEntries = new ConcurrentLinkedQueue<>();
    
    private final ConcurrentHashMap<String, List<Map<String, Object>>> traceGroups = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> traceTimestamps = new ConcurrentHashMap<>();
    
    private final AtomicInteger captureCounter = new AtomicInteger(0);
    
    private volatile int batchSize = 100;
    private volatile int entryCountThreshold = 50;
    
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
     * @param entryThreshold fallback threshold for entries without traceId
     * @param batchSize maximum number of entries to send in one batch
     */
    public synchronized void initialize(final Sink<Record<Event>> sink, 
                                     final int entryThreshold, 
                                     final int batchSize) {
        if (initialized.get()) {
            LOG.warn("LiveCaptureOutputManager already initialized, ignoring duplicate initialization");
            return;
        }
        
        if (sink == null) {
            LOG.error("Cannot initialize LiveCaptureOutputManager with null sink");
            return;
        }
        
        this.outputSink = sink;
        this.entryCountThreshold = Math.max(1, entryThreshold);
        this.batchSize = Math.max(1, batchSize);
        
        // Initialize the sink
        try {
            sink.initialize();
            LOG.info("LiveCaptureOutputManager initialized with sink: {}, entryThreshold: {}, batchSize: {}", 
                    sink.getClass().getSimpleName(), this.entryCountThreshold, this.batchSize);
            initialized.set(true);
        } catch (Exception e) {
            LOG.error("Failed to initialize live capture output sink: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Enables live capture output processing.
     * Sets up count-based processing instead of time-based.
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
     * Processes any remaining entries before disabling.
     */
    public synchronized void disable() {
        if (enabled.get()) {
            enabled.set(false);
            
            // Process any remaining pending entries
            if (!pendingEntries.isEmpty()) {
                processAndOutputEntries();
            }
            
            // Process any remaining trace groups
            processAllPendingTraces();
            
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
    
    public void addEntry(final Map<String, Object> entry) {
        if (!isEnabled()) return;
        
        String traceId = (String) entry.get("traceId");
        
        if (traceId == null) {
            pendingEntries.offer(entry);
            if (pendingEntries.size() >= entryCountThreshold) {
                processAndOutputEntries();
            }
            return;
        }
        
        traceGroups.computeIfAbsent(traceId, k -> new ArrayList<>()).add(entry);
        traceTimestamps.put(traceId, System.currentTimeMillis());
        
        if ("Sink".equals(entry.get("stage"))) {
            processTrace(traceId, false);
        }
        
        cleanupTimedOutTraces();
    }
    
    private void processAndOutputEntries() {
        if (!isEnabled()) return;
        
        try {
            List<Map<String, Object>> entries = new ArrayList<>();
            Map<String, Object> entry;
            while (entries.size() < batchSize && (entry = pendingEntries.poll()) != null) {
                entries.add(entry);
            }
            
            if (!entries.isEmpty()) {
                List<Record<Event>> records = entries.stream()
                    .map(this::createSimpleEvent)
                    .map(Record::new)
                    .collect(java.util.stream.Collectors.toList());
                outputSink.output(records);
                LOG.debug("Sent {} entries to output sink", records.size());
            }
        } catch (Exception e) {
            LOG.error("Error processing entries: {}", e.getMessage(), e);
        }
    }
    
    private Event createSimpleEvent(Map<String, Object> entry) {
        Event event = JacksonEvent.builder()
            .withEventType("live_capture_output")
            .withTimeReceived(Instant.now())
            .withData(entry)
            .build();
        
        event.getMetadata().setAttribute("event_source", "live_capture_output");
        
        return event;
    }
    
    private void processTrace(String traceId, boolean timedOut) {
        List<Map<String, Object>> entries = traceGroups.remove(traceId);
        traceTimestamps.remove(traceId);
        
        if (entries == null || entries.isEmpty()) return;
        
        try {
            entries.sort((e1, e2) -> {
                String time1 = (String) e1.get("eventTime");
                String time2 = (String) e2.get("eventTime");
                return time1 != null && time2 != null ? time1.compareTo(time2) : 0;
            });
            
            Map<String, Object> traceData = new HashMap<>();
            traceData.put("Capture #", captureCounter.incrementAndGet());
            traceData.put("entries", cleanEntries(entries));
            if (timedOut) traceData.put("timedOut", true);
            
            String eventType = timedOut ? "live_capture_incomplete_trace" : "live_capture_complete_trace";
            Event event = JacksonEvent.builder()
                .withEventType(eventType)
                .withTimeReceived(Instant.now())
                .withData(traceData)
                .build();
            
            event.getMetadata().setAttribute("event_source", eventType);
            event.getMetadata().setAttribute("trace_id", traceId);
            event.getMetadata().setAttribute("entry_count", entries.size());
            
            outputSink.output(List.of(new Record<>(event)));
            LOG.debug("Sent {} trace {} with {} entries", timedOut ? "incomplete" : "complete", traceId, entries.size());
        } catch (Exception e) {
            LOG.error("Error processing trace {}: {}", traceId, e.getMessage(), e);
        }
    }
    
    private List<Map<String, Object>> cleanEntries(List<Map<String, Object>> entries) {
        return entries.stream().map(entry -> {
            Map<String, Object> clean = new HashMap<>(entry);
            clean.remove("traceId");
            return clean;
        }).collect(java.util.stream.Collectors.toList());
    }
    
    private void cleanupTimedOutTraces() {
        long currentTime = System.currentTimeMillis();
        
        traceTimestamps.entrySet().removeIf(entry -> {
            if (currentTime - entry.getValue() > 30000) {
                LOG.warn("Trace {} timed out after {}ms", entry.getKey(), 30000);
                processTrace(entry.getKey(), true);
                return true;
            }
            return false;
        });
    }
    private void processAllPendingTraces() {
        new ArrayList<>(traceGroups.keySet()).forEach(traceId -> processTrace(traceId, true));
    }
}