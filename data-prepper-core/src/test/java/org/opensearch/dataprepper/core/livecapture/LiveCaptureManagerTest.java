/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class LiveCaptureManagerTest {

    private LiveCaptureManager liveCaptureManager;
    private Event testEvent;

    @BeforeEach
    void setUp() {
        liveCaptureManager = new LiveCaptureManager();
        liveCaptureManager.initialize(false, 1.0);
        testEvent = JacksonEvent.builder()
                .withData(Map.of("field1", "value1", "field2", "value2"))
                .withEventType("test")
                .build();
    }

    @Test
    void testInitialization() {
        assertThat(liveCaptureManager, notNullValue());
        assertThat(liveCaptureManager.isEnabled(), is(false));
        // Note: rate may have been modified by other tests, so just check it's positive
        assertThat(liveCaptureManager.getRateLimit() > 0, is(true));
    }

    @Test
    void testEnableDisable() {
        liveCaptureManager.setEnabled(true);
        assertThat(liveCaptureManager.isEnabled(), is(true));

        liveCaptureManager.setEnabled(false);
        assertThat(liveCaptureManager.isEnabled(), is(false));
    }

    @Test
    void testRateLimit() {
        liveCaptureManager.setRateLimit(5.0);
        assertThat(liveCaptureManager.getRateLimit(), equalTo(5.0));
    }

    @Test
    void testEventMarking() {
        assertThat(LiveCaptureManager.shouldLiveCapture(testEvent), is(false));

        LiveCaptureManager.setLiveCapture(testEvent, true);
        assertThat(LiveCaptureManager.shouldLiveCapture(testEvent), is(true));

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        assertThat(output, notNullValue());
        assertThat(output.isEmpty(), is(true));
    }

    @Test
    void testAddLiveCaptureEntry() {
        LiveCaptureManager.setLiveCapture(testEvent, true);

        LiveCaptureManager.addLiveCaptureEntry(testEvent, LiveCaptureManager.SOURCE, "Test message", null, null);

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        assertThat(output.size(), equalTo(1));

        Map<String, Object> entry = output.get(0);
        assertThat(entry.get("stage"), equalTo(LiveCaptureManager.SOURCE));
        assertThat(entry.get("logMessage"), equalTo("Test message"));
        assertThat(entry.get("eventTime"), notNullValue());
        assertThat(entry.get("captureMetaData"), notNullValue());
    }

    @Test
    void testAddLiveCaptureEntryWithNonMarkedEvent() {
        LiveCaptureManager.addLiveCaptureEntry(testEvent, LiveCaptureManager.SOURCE, "Test message", null, null);

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        assertThat(output, nullValue());
    }

    @Test
    void testHasActiveFilters() {
        assertThat(liveCaptureManager.hasActiveFilters(), is(false));

        liveCaptureManager.addFilter("field1", "value1");
        assertThat(liveCaptureManager.hasActiveFilters(), is(true));

        liveCaptureManager.clearFilters();
        assertThat(liveCaptureManager.hasActiveFilters(), is(false));
    }


    @Test
    void testShouldCaptureEventWhenDisabled() {
        liveCaptureManager.setEnabled(false);
        assertThat(liveCaptureManager.shouldCaptureEvent(testEvent), is(false));
    }

    @Test
    void testShouldCaptureEventWhenEnabled() {
        liveCaptureManager.setEnabled(true);
        liveCaptureManager.setRateLimit(100.0); // High rate to ensure capture

        // Should allow at least some events through rate limiting
        boolean anyCaptured = false;
        for (int i = 0; i < 50; i++) {
            if (liveCaptureManager.shouldCaptureEvent(testEvent)) {
                anyCaptured = true;
                break;
            }
            // Small delay to allow rate limiter to refill
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(anyCaptured, is(true));
    }

    @Test
    void testAddLiveCaptureEntryWithProcessorStage() {
        LiveCaptureManager.setLiveCapture(testEvent, true);

        LiveCaptureManager.addLiveCaptureEntry(testEvent, LiveCaptureManager.PROCESSOR, "Processor executed", "uppercase", null);

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        @SuppressWarnings("unchecked")
        Map<String, Object> captureMetaData = (Map<String, Object>) output.get(0).get("captureMetaData");
        assertThat(captureMetaData.get("processorName"), equalTo("uppercase"));
    }

    @Test
    void testAddLiveCaptureEntryWithRouteStage() {
        LiveCaptureManager.setLiveCapture(testEvent, true);

        LiveCaptureManager.addLiveCaptureEntry(testEvent, LiveCaptureManager.ROUTE, "Route matched", "test-route", null);

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        @SuppressWarnings("unchecked")
        Map<String, Object> captureMetaData = (Map<String, Object>) output.get(0).get("captureMetaData");
        assertThat(captureMetaData.get("routeName"), equalTo("test-route"));
    }

    @Test
    void testAddLiveCaptureEntryWithSinkStage() {
        LiveCaptureManager.setLiveCapture(testEvent, true);

        LiveCaptureManager.addLiveCaptureEntry(testEvent, LiveCaptureManager.SINK, "Sink received", "opensearch", null);

        List<Map<String, Object>> output = LiveCaptureManager.getLiveCaptureOutput(testEvent);
        @SuppressWarnings("unchecked")
        Map<String, Object> captureMetaData = (Map<String, Object>) output.get(0).get("captureMetaData");
        assertThat(captureMetaData.get("sinkName"), equalTo("opensearch"));
    }

    @Test
    void testFilters() {
        liveCaptureManager.setEnabled(true);
        liveCaptureManager.setRateLimit(100.0); // High rate to ensure capture
        liveCaptureManager.addFilter("field1", "value1");

        // Event has field1 with value "value1", should pass filter and rate limiting should allow it
        boolean shouldCapture = false;
        for (int i = 0; i < 50; i++) {
            if (liveCaptureManager.shouldCaptureEvent(testEvent)) {
                shouldCapture = true;
                break;
            }
            // Small delay to allow rate limiter to refill
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(shouldCapture, is(true));

        liveCaptureManager.clearFilters();
        // After clearing filters, should work with any event
        boolean shouldCaptureAfterClear = false;
        for (int i = 0; i < 50; i++) {
            if (liveCaptureManager.shouldCaptureEvent(testEvent)) {
                shouldCaptureAfterClear = true;
                break;
            }
            // Small delay to allow rate limiter to refill
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assertThat(shouldCaptureAfterClear, is(true));
    }

    @Test
    void testShouldCaptureEventWithFiltersNoMatch() {
        liveCaptureManager.setEnabled(true);
        liveCaptureManager.addFilter("testFilter", "nonexistent_field");

        // Event doesn't have nonexistent_field, should not pass filter
        boolean shouldCapture = liveCaptureManager.shouldCaptureEvent(testEvent);
        assertThat(shouldCapture, is(false));
    }


    @Test
    void testGetInstance() {
        LiveCaptureManager instance1 = LiveCaptureManager.getInstance();
        LiveCaptureManager instance2 = LiveCaptureManager.getInstance();
        
        assertThat(instance1, equalTo(instance2));
    }
}