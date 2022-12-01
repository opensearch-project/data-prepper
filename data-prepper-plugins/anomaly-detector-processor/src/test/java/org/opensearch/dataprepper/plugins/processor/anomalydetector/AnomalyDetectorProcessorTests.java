/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AnomalyDetectorProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AnomalyDetectorProcessorConfig mockConfig;

    private AnomalyDetectorProcessor anomalyDetectorProcessor;

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

    @Test
    void testAnomalyDetectorProcessor() {
        when(mockConfig.getKeys()).thenReturn(new ArrayList<String>(Collections.singleton("latency")));
        when(mockConfig.getMode()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_MODE);
        when(mockConfig.getShingleSize()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_SHINGLE_SIZE);
        when(mockConfig.getSampleSize()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_SAMPLE_SIZE);
        when(mockConfig.getTimeDecay()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_TIME_DECAY);
        anomalyDetectorProcessor = new AnomalyDetectorProcessor(pluginMetrics, mockConfig);
        final int numSamples = 1024;
        for (int i = 0; i < numSamples; i++) {
            final List<Record<Event>> records = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6))));
        }
        final List<Record<Event>> records = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(10.4, 10.8))));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        double deviation = (double)event.get(AnomalyDetectorProcessor.DEVIATION_KEY, Double.class);
        assertThat(deviation, greaterThan(9.0));
        double grade = (double)event.get(AnomalyDetectorProcessor.GRADE_KEY, Double.class);
        assertThat(deviation, greaterThan(1.0));
    }

    @Test
    void testAnomalyDetectorProcessorTwoKeys() {
        List<String> keyList = new ArrayList<String>();
        keyList.add("latency");
        keyList.add("bytes");
        when(mockConfig.getKeys()).thenReturn(keyList);
        when(mockConfig.getMode()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_MODE);
        when(mockConfig.getShingleSize()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_SHINGLE_SIZE);
        when(mockConfig.getSampleSize()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_SAMPLE_SIZE);
        when(mockConfig.getTimeDecay()).thenReturn(AnomalyDetectorProcessorConfig.DEFAULT_TIME_DECAY);
        anomalyDetectorProcessor = new AnomalyDetectorProcessor(pluginMetrics, mockConfig);
        final int numSamples = 1024;
        for (int i = 0; i < numSamples; i++) {
            final List<Record<Event>> records = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(0.5, 0.6), ThreadLocalRandom.current().nextLong(100, 110))));
        }
        final List<Record<Event>> records = (List<Record<Event>>) anomalyDetectorProcessor.doExecute(Collections.singletonList(getLatencyBytesMessage(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextDouble(10.4, 10.8), ThreadLocalRandom.current().nextLong(1000, 1110))));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        double deviation = (double)event.get(AnomalyDetectorProcessor.DEVIATION_KEY, Double.class);
        assertThat(deviation, greaterThan(9.0));
        double grade = (double)event.get(AnomalyDetectorProcessor.GRADE_KEY, Double.class);
        assertThat(deviation, greaterThan(1.0));
    }

}
