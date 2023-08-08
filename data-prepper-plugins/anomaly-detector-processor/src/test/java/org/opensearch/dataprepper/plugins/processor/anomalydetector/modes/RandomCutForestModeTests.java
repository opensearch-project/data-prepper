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
import java.util.Collection;
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

import static org.mockito.Mockito.mock;
import org.mockito.Mock;
import static org.mockito.Mockito.when;


import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.DEVIATION_KEY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.AnomalyDetectorProcessor.GRADE_KEY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_SHINGLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_SAMPLE_SIZE;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_TIME_DECAY;
import static org.opensearch.dataprepper.plugins.processor.anomalydetector.modes.RandomCutForestModeConfig.DEFAULT_OUTPUT_AFTER;

@ExtendWith(MockitoExtension.class)
public class RandomCutForestModeTests {
    @Mock
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

    private Record<Event> getMessageFloatLong(String message, String floatName, String longName, double latency, long bytes) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put(floatName, latency);
        testData.put(longName, bytes);
        return buildRecordWithEvent(testData);
    }
    
    private Record<Event> getHourlyLoadMessage(String message, double load, int hour) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put("load", load);
        testData.put("hour", hour);
        return buildRecordWithEvent(testData);
    }
    
    @BeforeEach
    void setup() {
        randomCutForestModeConfig = mock(RandomCutForestModeConfig.class);
        when(randomCutForestModeConfig.getSampleSize()).thenReturn(DEFAULT_SAMPLE_SIZE);
        when(randomCutForestModeConfig.getShingleSize()).thenReturn(DEFAULT_SHINGLE_SIZE);
        when(randomCutForestModeConfig.getOutputAfter()).thenReturn(DEFAULT_OUTPUT_AFTER);
        when(randomCutForestModeConfig.getTimeDecay()).thenReturn(DEFAULT_TIME_DECAY);
    }

    private RandomCutForestMode createObjectUnderTest() {
        return new RandomCutForestMode(randomCutForestModeConfig);
    }

    @Test
    void testRandomCutForestMode() {
        randomCutForestMode = createObjectUnderTest();
        List<String> keys = new ArrayList<String>(Collections.singleton("latency"));
        randomCutForestMode.initialize(keys, false);
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
        randomCutForestMode = createObjectUnderTest();
        List<String> keyList = new ArrayList<String>();
        String floatFieldName = "latency";
        String longFieldName = "bytes";
        keyList.add(floatFieldName);
        keyList.add(longFieldName);
        randomCutForestMode.initialize(keyList, false);
        final int numSamples = 1024;
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getMessageFloatLong(UUID.randomUUID().toString(), floatFieldName, longFieldName, ThreadLocalRandom.current().nextDouble(0.5, 0.6), ThreadLocalRandom.current().nextLong(100,110)));
        }
        randomCutForestMode.handleEvents(records);
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        recordsWithAnomaly.add(getMessageFloatLong(UUID.randomUUID().toString(), floatFieldName, longFieldName, ThreadLocalRandom.current().nextDouble(15.4, 15.8), ThreadLocalRandom.current().nextLong(1000,1110)));
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

    @Test
    void testRandomCutForestModeWithOutputAfter() {
        double dailyLoads[] = {5.0, 4.0, 3.0, 2.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 9.5, 8.5, 7.5, 6.5, 6.0, 6.5, 7.0, 7.5, 9.5, 11.0, 12.5, 10.5, 8.5, 7.0};
        when(randomCutForestModeConfig.getSampleSize()).thenReturn(1024);
        when(randomCutForestModeConfig.getShingleSize()).thenReturn(8);
        when(randomCutForestModeConfig.getOutputAfter()).thenReturn(1024);
        
        randomCutForestMode = createObjectUnderTest();
        List<String> keyList = new ArrayList<String>();
        String floatFieldName = "load";
        String longFieldName = "hour";
        keyList.add(floatFieldName);
        keyList.add(longFieldName); 
        randomCutForestMode.initialize(keyList, false);
        final int numSamples = (365+200)*24+4; // number of samples more than a year
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        long hour = 0;
        for (int i = 0; i < numSamples; i++) {
            records.add(getMessageFloatLong(UUID.randomUUID().toString(), floatFieldName, longFieldName, dailyLoads[i % 24], hour++ % 168));
        }
        final Collection<Record<Event>> outputRecords = randomCutForestMode.handleEvents(records);
        assertThat(outputRecords.size(), equalTo(0));
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        recordsWithAnomaly.add(getMessageFloatLong(UUID.randomUUID().toString(), floatFieldName, longFieldName, 12.5, hour % 168));

        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), equalTo(1));
    }

    @Test
    void testRandomCutForestModeVerboseTrue() {
        randomCutForestMode = createObjectUnderTest();
        List<String> keys = new ArrayList<String>(Collections.singleton("latency"));
        randomCutForestMode.initialize(keys, true);
        final int numSamples = 1024;
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6)));
        }
        randomCutForestMode.handleEvents(records);
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            recordsWithAnomaly.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(1, 1.1)));
        }

        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), greaterThan(5));
    }

    @Test
    void testRandomCutForestModeVerboseFalse() {
        randomCutForestMode = createObjectUnderTest();
        List<String> keys = new ArrayList<String>(Collections.singleton("latency"));
        randomCutForestMode.initialize(keys, false);
        final int numSamples = 1024;
        List<Record<Event>> records = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            records.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6)));
        }
        randomCutForestMode.handleEvents(records);
        final List<Record<Event>> recordsWithAnomaly = new ArrayList<Record<Event>>();
        for (int i = 0; i < numSamples; i++) {
            recordsWithAnomaly.add(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(1, 1.1)));
        }

        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), equalTo(1));
    }
}
