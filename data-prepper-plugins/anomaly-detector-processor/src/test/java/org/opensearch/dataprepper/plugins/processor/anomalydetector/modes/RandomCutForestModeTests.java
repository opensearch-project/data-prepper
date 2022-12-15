/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector.modes;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import static java.util.stream.Collectors.toList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.DEVIATION_KEY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.GRADE_KEY;

@ExtendWith(MockitoExtension.class)
public class RandomCutForestModeTests {
    RandomCutForestModeConfig randomCutForestModeConfig;
    RandomCutForestMode randomCutForestMode;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Record<Event> getLatencyMessage(String message, double latency) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("latency", latency);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getLatencyBytesMessage(String message, double latency, long bytes) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("latency", latency);
        testData.put("bytes", bytes);
        return buildRecordWithEvent(testData);
    }
    
    @BeforeEach
    void setup() {
        randomCutForestModeConfig = new RandomCutForestModeConfig();
        randomCutForestMode = new RandomCutForestMode(randomCutForestModeConfig);
    }

    @Test
    void testRandomCutForestMode() {
        List<String> keys = new ArrayList<String>(Collections.singleton("latency"));
        randomCutForestMode.initialize(keys);
        final int numSamples = 1024;
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6)));
        }
        randomCutForestMode.handleEvents(records);
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        recordsWithAnomaly.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(10.4, 10.8)));
        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), equalTo(1));
        Event event = anomalyRecords.get(0).getData();
        List<Double> deviation = event.get(DEVIATION_KEY, List.class);
        assertThat((double)deviation.get(0), greaterThan(9.0));
        double grade = (double)event.get(GRADE_KEY, Double.class);
        assertThat(grade, equalTo(1.0));
    }

    @Test
    void testRandomCutForestModeMultipleKeys() {
        List<String> keyList = new ArrayList<String>();
        keyList.add("latency");
        keyList.add("bytes");
        randomCutForestMode.initialize(keyList);
        final int numSamples = 1024;
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6), ThreadLocalRandom.current().nextLong(100,110)));
        }
        randomCutForestMode.handleEvents(records);
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        recordsWithAnomaly.add(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(10.4, 10.8), ThreadLocalRandom.current().nextLong(1000,1110)));
        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), equalTo(1));
        Event event = anomalyRecords.get(0).getData();
        List<Double> deviation = event.get(DEVIATION_KEY, List.class);
        for (int i = 0; i < keyList.size(); i++) {
            assertThat((double)deviation.get(i), greaterThan(9.0));
        }
        double grade = (double)event.get(GRADE_KEY, Double.class);
        assertThat(grade, equalTo(1.0));
    }
}
