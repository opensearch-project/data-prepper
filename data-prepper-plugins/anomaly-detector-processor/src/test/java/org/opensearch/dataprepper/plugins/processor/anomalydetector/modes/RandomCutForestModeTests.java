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
        System.out.println("......."+event.toJsonString());
        List<Double> deviation = event.get(DEVIATION_KEY, List.class);
        for (int i = 0; i < keyList.size(); i++) {
            System.out.println("====i=="+i+"====deviation==="+deviation.get(i));
            assertThat((double)deviation.get(i), greaterThan(9.0));
        }
        double grade = (double)event.get(GRADE_KEY, Double.class);
        assertThat(grade, equalTo(1.0));
    }

/*
    @Test
    void testRandomCutForestModeWithOutputAfter() {
        double dailyLoads[] = {5.0, 4.0, 3.0, 2.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 9.5, 8.5, 7.5, 6.5, 6.0, 6.5, 7.0, 7.5, 9.5, 11.0, 12.5, 10.5, 8.5, 7.0};
        List<String> keyList = new ArrayList<String>();
        keyList.add("load");
        keyList.add("week_hour"); // Hour number from the beginning of the week. range 0 through 167
        randomCutForestMode.initialize(keyList);
        double load;
        final int numSamples = 2*365*24; // two years, hourly loads
        for (int i = 0; i < numSamples; i++) {
            // 168 = 24 * 7 = number of hours in a week
            if ((i == (365+200)*24) && (i % 24) == 4) {
                load = 12.5;
            } else {
                load = dailyLoads[i % 24];
            }

            records.add(getHourlyLoadMessage(UUID.randomUUID().toString(), load, i % 168);
        final List<Record<Event>> anomalyRecords = randomCutForestMode.handleEvents(recordsWithAnomaly).stream().collect(toList());;
        assertThat(anomalyRecords.size(), greaterThanOrEqual(1));
        }
        randomCutForestMode.handleEvents(records);
    }
*/
}
